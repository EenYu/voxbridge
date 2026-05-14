package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"voxbridge/internal/config"
	"voxbridge/internal/dialog"
	"voxbridge/internal/freeswitch"
	"voxbridge/internal/llm"
	"voxbridge/internal/media"
	"voxbridge/internal/speech"
	"voxbridge/internal/speech/volcengine"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("voxbridge stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	chat, err := llm.NewOpenAICompatibleClient(llm.OpenAIOptions{
		BaseURL: cfg.LLM.BaseURL,
		APIKey:  cfg.LLM.APIKey,
		Model:   cfg.LLM.Model,
		Timeout: cfg.LLM.Timeout,
	})
	if err != nil {
		return err
	}

	stt, err := volcengine.NewSTTProvider(volcengine.STTConfig{
		Endpoint:               cfg.Volcengine.STTEndpoint,
		AppID:                  cfg.Volcengine.AppID,
		Token:                  volcToken(cfg),
		AccessKey:              volcAccessKey(cfg),
		ResourceID:             firstNonEmpty(cfg.Volcengine.STTResourceID, cfg.Volcengine.ResourceID),
		Cluster:                firstNonEmpty(cfg.Volcengine.STTCluster, cfg.Volcengine.Cluster),
		AsyncEndWindowSize:     cfg.Volcengine.STTAsyncEndWindowSize,
		AsyncForceToSpeechTime: cfg.Volcengine.STTAsyncForceToSpeechTime,
		Audio: volcengine.AudioConfig{
			Format:   "pcm",
			Codec:    "raw",
			Rate:     cfg.Audio.SampleRateHz,
			Bits:     16,
			Channel:  cfg.Audio.Channels,
			Language: "zh-CN",
		},
		Timeout: cfg.Timeouts.Speech,
		Logger:  logger,
	})
	if err != nil {
		return err
	}

	tts, err := volcengine.NewTTSProvider(volcengine.TTSConfig{
		Endpoint:   cfg.Volcengine.TTSEndpoint,
		AppID:      cfg.Volcengine.AppID,
		Token:      volcToken(cfg),
		AccessKey:  volcAccessKey(cfg),
		ResourceID: firstNonEmpty(cfg.Volcengine.TTSResourceID, cfg.Volcengine.ResourceID),
		Cluster:    firstNonEmpty(cfg.Volcengine.TTSCluster, cfg.Volcengine.Cluster),
		VoiceType:  cfg.Volcengine.VoiceType,
		Audio: volcengine.AudioConfig{
			Format:    "pcm",
			Encoding:  "pcm",
			Rate:      cfg.Audio.SampleRateHz,
			Bits:      16,
			Channel:   cfg.Audio.Channels,
			VoiceType: cfg.Volcengine.VoiceType,
		},
		Timeout: cfg.Timeouts.Speech,
		Logger:  logger,
	})
	if err != nil {
		return err
	}

	fsClient, err := freeswitch.NewClient(freeswitch.Config{
		Address:               cfg.ESL.Address,
		Password:              cfg.ESL.Password,
		PublicWSURL:           cfg.PublicWS.URL,
		RequiredVariableName:  "voxbridge_enabled",
		RequiredVariableValue: "true",
	}, logger)
	if err != nil {
		return err
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry := newSessionRegistry(stt, tts, chat, fsClient, cfg.Audio.SampleRateHz, logger)
	mediaHandler := media.NewHandler(func(ctx context.Context, info media.ConnectionInfo, sender dialog.AudioSender) (media.DialogSession, error) {
		logger.Info("media session requested", "uuid", info.CallUUID)
		session, err := registry.New(ctx, info, sender)
		if err != nil {
			logger.Warn("media session creation failed", "uuid", info.CallUUID, "error", err)
			return nil, err
		}
		return session, nil
	})
	mediaHandler.SampleRate = cfg.Audio.SampleRateHz
	mediaHandler.Channels = cfg.Audio.Channels
	mediaHandler.Logger = logger

	mux := http.NewServeMux()
	mux.Handle(media.MediaPath, mediaHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	server := &http.Server{
		Addr:              cfg.HTTP.Address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("media HTTP server starting", "addr", cfg.HTTP.Address, "path", media.MediaPath)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	go func() {
		errCh <- fsClient.Run(rootCtx)
	}()

	select {
	case <-rootCtx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			stop()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
			registry.CloseAll()
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	registry.CloseAll()
	return rootCtx.Err()
}

type sessionRegistry struct {
	stt    speech.STTProvider
	tts    speech.TTSProvider
	chat   llm.Client
	fs     *freeswitch.Client
	rate   int
	logger *slog.Logger

	mu       sync.Mutex
	sessions map[string]*trackedSession
	seq      int64
}

func newSessionRegistry(stt speech.STTProvider, tts speech.TTSProvider, chat llm.Client, fsClient *freeswitch.Client, sampleRate int, logger *slog.Logger) *sessionRegistry {
	return &sessionRegistry{
		stt:      stt,
		tts:      tts,
		chat:     chat,
		fs:       fsClient,
		rate:     sampleRate,
		logger:   logger,
		sessions: make(map[string]*trackedSession),
	}
}

func (r *sessionRegistry) New(ctx context.Context, info media.ConnectionInfo, sender dialog.AudioSender) (media.DialogSession, error) {
	id := strings.TrimSpace(info.CallUUID)
	if id == "" {
		id = r.nextAnonymousID()
	}
	r.logger.Info("starting dialog session", "uuid", id)

	r.mu.Lock()
	existing := r.sessions[id]
	r.mu.Unlock()
	if existing != nil {
		_ = existing.Close()
	}

	playbackSender := sender
	if r.fs != nil && strings.TrimSpace(info.CallUUID) != "" {
		playbackSender = newFreeSwitchAudioSender(r.fs, id, r.rate, r.logger)
		r.logger.Info("using freeswitch playback sender", "uuid", id, "sample_rate", r.rate)
	}

	session, err := dialog.NewSession(ctx, id, r.stt, r.tts, r.chat, playbackSender, dialog.Options{Logger: r.logger})
	if err != nil {
		return nil, err
	}

	tracked := &trackedSession{
		id:       id,
		session:  session,
		registry: r,
	}

	r.mu.Lock()
	r.sessions[id] = tracked
	r.mu.Unlock()
	r.logger.Info("media session created", "uuid", id)
	return tracked, nil
}

func (r *sessionRegistry) CloseAll() {
	r.mu.Lock()
	sessions := make([]*trackedSession, 0, len(r.sessions))
	for _, session := range r.sessions {
		sessions = append(sessions, session)
	}
	r.sessions = make(map[string]*trackedSession)
	r.mu.Unlock()
	for _, session := range sessions {
		_ = session.closeWithoutRegistry()
	}
}

func (r *sessionRegistry) nextAnonymousID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	return "media-" + strconv.FormatInt(r.seq, 10)
}

type trackedSession struct {
	id       string
	session  *dialog.Session
	registry *sessionRegistry
	once     sync.Once
}

func (s *trackedSession) WritePCM(ctx context.Context, pcm []byte) error {
	return s.session.WritePCM(ctx, pcm)
}

func (s *trackedSession) Close() error {
	var err error
	s.once.Do(func() {
		err = s.session.Close()
		s.registry.mu.Lock()
		if s.registry.sessions[s.id] == s {
			delete(s.registry.sessions, s.id)
		}
		s.registry.mu.Unlock()
		s.registry.logger.Info("media session closed", "uuid", s.id)
	})
	return err
}

func (s *trackedSession) closeWithoutRegistry() error {
	var err error
	s.once.Do(func() {
		err = s.session.Close()
		s.registry.logger.Info("media session closed", "uuid", s.id)
	})
	return err
}

func volcToken(cfg config.Config) string {
	return strings.TrimSpace(cfg.Volcengine.Token)
}

func volcAccessKey(cfg config.Config) string {
	return firstNonEmpty(cfg.Volcengine.AccessKey, cfg.Volcengine.SecretAccessKey, cfg.Volcengine.AccessKeyID)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
