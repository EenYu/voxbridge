package dialog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"voxbridge/internal/llm"
	"voxbridge/internal/speech"
)

const (
	defaultSystemPrompt                   = "你是一名简洁、礼貌、可靠的中文电话助手。回答要短，优先直接解决来电者的问题。"
	defaultHistoryTurns                   = 6
	defaultMaxTTSChars                    = 80
	defaultMinTTSChars                    = 12
	defaultFirstTTSPlaybackChunkMS        = 20
	defaultSubsequentTTSPlaybackChunkMS   = 100
	defaultFirstTTSPlaybackChunkSize      = 16000 * 2 * defaultFirstTTSPlaybackChunkMS / 1000
	defaultSubsequentTTSPlaybackChunkSize = 16000 * 2 * defaultSubsequentTTSPlaybackChunkMS / 1000
)

type Options struct {
	SystemPrompt string
	HistoryTurns int
	MaxTTSChars  int
	MinTTSChars  int
	Logger       *slog.Logger
}

type Session struct {
	id     string
	stt    speech.STTSession
	tts    speech.TTSProvider
	llm    llm.Client
	sender AudioSender
	opts   Options

	ctx    context.Context
	cancel context.CancelFunc

	mu             sync.Mutex
	history        []llm.Message
	activeCancel   context.CancelFunc
	activeDone     chan struct{}
	closed         bool
	responseSerial int64
	partialEvents  int

	clearQueuedRequests  uint64
	clearQueuedProcessed uint64
	clearQueuedRunning   bool
}

func NewSession(ctx context.Context, id string, sttProvider speech.STTProvider, tts speech.TTSProvider, chat llm.Client, sender AudioSender, opts Options) (*Session, error) {
	if sttProvider == nil {
		return nil, errors.New("dialog: missing STT provider")
	}
	if tts == nil {
		return nil, errors.New("dialog: missing TTS provider")
	}
	if chat == nil {
		return nil, errors.New("dialog: missing LLM client")
	}
	if sender == nil {
		return nil, errors.New("dialog: missing audio sender")
	}
	if strings.TrimSpace(opts.SystemPrompt) == "" {
		opts.SystemPrompt = defaultSystemPrompt
	}
	if opts.HistoryTurns <= 0 {
		opts.HistoryTurns = defaultHistoryTurns
	}
	if opts.MaxTTSChars <= 0 {
		opts.MaxTTSChars = defaultMaxTTSChars
	}
	if opts.MinTTSChars <= 0 {
		opts.MinTTSChars = defaultMinTTSChars
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	sttSession, err := sttProvider.Start(sessionCtx, id)
	if err != nil {
		cancel()
		return nil, err
	}

	s := &Session{
		id:     id,
		stt:    sttSession,
		tts:    tts,
		llm:    chat,
		sender: sender,
		opts:   opts,
		ctx:    sessionCtx,
		cancel: cancel,
		history: []llm.Message{{
			Role:    llm.RoleSystem,
			Content: opts.SystemPrompt,
		}},
	}
	go s.consumeRecognition()
	return s, nil
}

func (s *Session) ID() string {
	return s.id
}

func (s *Session) WritePCM(ctx context.Context, pcm []byte) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return context.Canceled
	}
	return s.stt.WritePCM(ctx, pcm)
}

func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancelActiveLocked()
	s.cancel()
	s.mu.Unlock()
	return s.stt.Close()
}

func (s *Session) consumeRecognition() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case ev, ok := <-s.stt.Events():
			if !ok {
				s.opts.Logger.Info("stt events closed", "session", s.id, "context_error", s.ctx.Err())
				return
			}
			s.handleRecognition(ev)
		}
	}
}

func (s *Session) handleRecognition(ev speech.RecognitionEvent) {
	text := strings.TrimSpace(ev.Text)
	if text == "" {
		return
	}

	switch ev.Kind {
	case speech.RecognitionPartial:
		s.partialEvents++
		if s.partialEvents <= 5 || s.partialEvents%20 == 0 {
			s.opts.Logger.Info("recognized partial speech",
				"session", s.id,
				"partials", s.partialEvents,
				"chars", len([]rune(text)),
				"text", previewText(text, 120),
			)
		}
		s.mu.Lock()
		s.cancelActiveLocked()
		s.mu.Unlock()
	case speech.RecognitionFinal:
		asrFinalAt := ev.ReceivedAt
		if asrFinalAt.IsZero() {
			asrFinalAt = time.Now()
		}
		s.opts.Logger.Info("recognized final speech",
			"session", s.id,
			"chars", len([]rune(text)),
			"text", previewText(text, 200),
		)
		s.startUserTurn(text, asrFinalAt)
	}
}

func (s *Session) startUserTurn(text string, asrFinalAt time.Time) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.cancelActiveLocked()
	s.history = trimHistory(append(s.history, llm.Message{Role: llm.RoleUser, Content: text}), s.opts.HistoryTurns)
	messages := append([]llm.Message(nil), s.history...)
	ctx, cancel := context.WithCancel(s.ctx)
	s.responseSerial++
	serial := s.responseSerial
	done := make(chan struct{})
	s.activeCancel = cancel
	s.activeDone = done
	s.mu.Unlock()

	go s.runAssistant(ctx, serial, messages, done, asrFinalAt)
}

func (s *Session) cancelActiveLocked() {
	if s.activeCancel != nil {
		s.activeCancel()
		s.activeCancel = nil
	}
	s.scheduleClearQueuedLocked()
}

func (s *Session) scheduleClearQueuedLocked() {
	s.clearQueuedRequests++
	if s.clearQueuedRunning {
		return
	}
	s.clearQueuedRunning = true
	go s.clearQueuedAsync()
}

func (s *Session) clearQueuedAsync() {
	for {
		s.mu.Lock()
		if s.clearQueuedProcessed >= s.clearQueuedRequests {
			s.clearQueuedRunning = false
			s.mu.Unlock()
			return
		}
		s.clearQueuedProcessed = s.clearQueuedRequests
		s.mu.Unlock()

		s.sender.ClearQueued()
	}
}

func (s *Session) runAssistant(ctx context.Context, serial int64, messages []llm.Message, done chan struct{}, asrFinalAt time.Time) {
	turn := newTurnMetrics(serial, asrFinalAt)
	ctx, cancelPipeline := context.WithCancel(ctx)
	defer cancelPipeline()
	defer close(done)
	defer func() {
		s.mu.Lock()
		if s.responseSerial == serial {
			s.activeCancel = nil
			s.activeDone = nil
		}
		s.mu.Unlock()
	}()

	s.opts.Logger.Info("llm stream requested", "session", s.id, "turn_id", serial, "elapsed_ms", turn.elapsedMS())
	deltas, errs := s.llm.StreamChat(ctx, messages)
	var assistant strings.Builder
	var speechBuf strings.Builder
	textQueue := make(chan string, 8)
	pcmQueue := make(chan []byte, 16)
	ttsDone := make(chan error, 1)
	playbackDone := make(chan error, 1)

	go s.runTTSWorker(ctx, turn, textQueue, pcmQueue, ttsDone)
	go s.runPlaybackWorker(ctx, turn, pcmQueue, playbackDone)

	flush := func() bool {
		fragment := strings.TrimSpace(speechBuf.String())
		if fragment == "" {
			speechBuf.Reset()
			return true
		}
		speechBuf.Reset()
		if turn.markFirstFragment() {
			s.opts.Logger.Info("first tts fragment ready",
				"session", s.id,
				"turn_id", turn.id,
				"elapsed_ms", turn.firstFragmentMS,
				"chars", len([]rune(fragment)),
				"text", previewText(fragment, 120),
			)
		}
		select {
		case <-ctx.Done():
			return false
		case err := <-ttsDone:
			if err != nil && ctx.Err() == nil {
				s.opts.Logger.Warn("tts worker failed", "session", s.id, "turn_id", turn.id, "error", err)
			}
			return false
		case err := <-playbackDone:
			if err != nil && ctx.Err() == nil {
				s.opts.Logger.Warn("playback worker failed", "session", s.id, "turn_id", turn.id, "error", err)
			}
			return false
		case textQueue <- fragment:
			s.opts.Logger.Info("tts fragment queued",
				"session", s.id,
				"turn_id", turn.id,
				"elapsed_ms", turn.elapsedMS(),
				"chars", len([]rune(fragment)),
				"text", previewText(fragment, 120),
			)
		}
		return true
	}

	textQueueClosed := false
	closeTextQueue := func() {
		if !textQueueClosed {
			close(textQueue)
			textQueueClosed = true
		}
	}

	for deltas != nil || errs != nil {
		select {
		case <-ctx.Done():
			return
		case err := <-ttsDone:
			if err != nil && ctx.Err() == nil {
				s.opts.Logger.Warn("tts worker failed", "session", s.id, "turn_id", turn.id, "error", err)
			}
			return
		case err := <-playbackDone:
			if err != nil && ctx.Err() == nil {
				s.opts.Logger.Warn("playback worker failed", "session", s.id, "turn_id", turn.id, "error", err)
			}
			return
		case delta, ok := <-deltas:
			if !ok {
				deltas = nil
				continue
			}
			if delta.Content != "" {
				if turn.markLLMFirstToken() {
					s.opts.Logger.Info("llm first token",
						"session", s.id,
						"turn_id", turn.id,
						"elapsed_ms", turn.llmFirstTokenMS,
					)
				}
				assistant.WriteString(delta.Content)
				speechBuf.WriteString(delta.Content)
			}
			if delta.Done || shouldFlushTTS(speechBuf.String(), s.opts.MaxTTSChars, s.opts.MinTTSChars) {
				if !flush() {
					return
				}
			}
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil && ctx.Err() == nil {
				s.opts.Logger.Warn("llm stream failed", "session", s.id, "error", err)
				return
			}
		}
	}

	if !flush() {
		return
	}
	closeTextQueue()
	if err := waitPipeline(ctx, cancelPipeline, ttsDone, playbackDone); err != nil {
		if ctx.Err() == nil || !errors.Is(err, ctx.Err()) {
			s.opts.Logger.Warn("assistant pipeline failed", "session", s.id, "turn_id", turn.id, "error", err)
		}
		return
	}
	if ctx.Err() != nil {
		return
	}

	text := strings.TrimSpace(assistant.String())
	if text == "" {
		return
	}
	s.opts.Logger.Info("assistant response completed",
		"session", s.id,
		"turn_id", turn.id,
		"elapsed_ms", turn.elapsedMS(),
		"chars", len([]rune(text)),
	)
	turn.logSummary(s.opts.Logger, s.id)
	s.mu.Lock()
	if s.responseSerial == serial && !s.closed {
		s.history = trimHistory(append(s.history, llm.Message{Role: llm.RoleAssistant, Content: text}), s.opts.HistoryTurns)
	}
	s.mu.Unlock()
}

func (s *Session) runTTSWorker(ctx context.Context, turn *turnMetrics, textQueue <-chan string, pcmQueue chan<- []byte, done chan<- error) {
	defer close(pcmQueue)
	fragmentID := 0
	firstPlaybackChunk := true
	for {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case text, ok := <-textQueue:
			if !ok {
				done <- nil
				return
			}
			fragmentID++
			if err := s.synthesizeToPCMChunks(ctx, turn, fragmentID, text, pcmQueue, &firstPlaybackChunk); err != nil {
				done <- err
				return
			}
		}
	}
}

func (s *Session) runPlaybackWorker(ctx context.Context, turn *turnMetrics, pcmQueue <-chan []byte, done chan<- error) {
	for {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
			return
		case pcm, ok := <-pcmQueue:
			if !ok {
				done <- nil
				return
			}
			if len(pcm) == 0 {
				continue
			}
			sendID, totalBytes := turn.addPlaybackSend(len(pcm))
			s.opts.Logger.Info("playback chunk send started",
				"session", s.id,
				"turn_id", turn.id,
				"elapsed_ms", turn.elapsedMS(),
				"send_id", sendID,
				"bytes", len(pcm),
			)
			if err := s.sender.SendPCM(ctx, pcm); err != nil {
				returnError(done, err)
				return
			}
			if turn.markFirstPlaybackQueued() {
				s.opts.Logger.Info("first playback queued",
					"session", s.id,
					"turn_id", turn.id,
					"elapsed_ms", turn.firstPlaybackQueuedMS,
				)
			}
			s.opts.Logger.Info("playback chunk queued",
				"session", s.id,
				"turn_id", turn.id,
				"elapsed_ms", turn.elapsedMS(),
				"send_id", sendID,
				"bytes", len(pcm),
				"total_bytes", totalBytes,
			)
		}
	}
}

func (s *Session) synthesizeToPCMChunks(ctx context.Context, turn *turnMetrics, fragmentID int, text string, out chan<- []byte, firstPlaybackChunk *bool) error {
	s.opts.Logger.Info("tts synthesize requested",
		"session", s.id,
		"turn_id", turn.id,
		"fragment_id", fragmentID,
		"elapsed_ms", turn.elapsedMS(),
		"chars", len([]rune(text)),
		"text", previewText(text, 200),
	)
	chunks, errs := s.tts.Synthesize(ctx, text)
	chunkCount := 0
	byteCount := 0
	var audio bytes.Buffer
	flushAudio := func(final bool) error {
		for {
			targetSize := defaultSubsequentTTSPlaybackChunkSize
			if firstPlaybackChunk != nil && *firstPlaybackChunk {
				targetSize = defaultFirstTTSPlaybackChunkSize
			}
			if audio.Len() < targetSize && !(final && audio.Len() > 0) {
				return nil
			}
			n := targetSize
			if audio.Len() < n {
				n = audio.Len()
			}
			if n%2 != 0 {
				return fmtPCM16OddLength(n)
			}
			pcm := copyBytes(audio.Next(n))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- pcm:
			}
			if firstPlaybackChunk != nil {
				*firstPlaybackChunk = false
			}
		}
	}
	for chunks != nil || errs != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-chunks:
			if !ok {
				chunks = nil
				continue
			}
			if len(chunk) == 0 {
				continue
			}
			chunkCount++
			byteCount += len(chunk)
			audio.Write(chunk)
			totalChunks, totalBytes := turn.addTTSChunk(len(chunk))
			if turn.markTTSFirstAudio() {
				s.opts.Logger.Info("tts first audio chunk",
					"session", s.id,
					"turn_id", turn.id,
					"fragment_id", fragmentID,
					"elapsed_ms", turn.ttsFirstAudioMS,
					"bytes", len(chunk),
				)
			}
			if chunkCount <= 3 || chunkCount%20 == 0 {
				s.opts.Logger.Info("tts audio chunk",
					"session", s.id,
					"turn_id", turn.id,
					"fragment_id", fragmentID,
					"chunks", chunkCount,
					"bytes", byteCount,
					"last_bytes", len(chunk),
					"turn_chunks", totalChunks,
					"turn_bytes", totalBytes,
				)
			}
			if err := flushAudio(false); err != nil {
				return err
			}
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil {
				return err
			}
		}
	}
	if chunkCount == 0 {
		s.opts.Logger.Warn("tts produced no audio", "session", s.id, "turn_id", turn.id, "fragment_id", fragmentID, "chars", len([]rune(text)))
		return nil
	}
	if err := flushAudio(true); err != nil {
		return err
	}
	s.opts.Logger.Info("tts audio queued",
		"session", s.id,
		"turn_id", turn.id,
		"fragment_id", fragmentID,
		"elapsed_ms", turn.elapsedMS(),
		"chunks", chunkCount,
		"bytes", byteCount,
	)
	return nil
}

func returnError(done chan<- error, err error) {
	done <- err
}

func waitPipeline(ctx context.Context, cancel context.CancelFunc, ttsDone, playbackDone <-chan error) error {
	for ttsDone != nil || playbackDone != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-ttsDone:
			ttsDone = nil
			if err != nil {
				cancel()
				return err
			}
		case err := <-playbackDone:
			playbackDone = nil
			if err != nil {
				cancel()
				return err
			}
		}
	}
	return nil
}

type turnMetrics struct {
	id         int64
	asrFinalAt time.Time

	mu                    sync.Mutex
	llmFirstTokenMS       int64
	firstFragmentMS       int64
	ttsFirstAudioMS       int64
	firstPlaybackQueuedMS int64
	ttsChunks             int
	ttsBytes              int
	playbackSends         int
	playbackBytes         int
}

func newTurnMetrics(id int64, asrFinalAt time.Time) *turnMetrics {
	if asrFinalAt.IsZero() {
		asrFinalAt = time.Now()
	}
	return &turnMetrics{
		id:                    id,
		asrFinalAt:            asrFinalAt,
		llmFirstTokenMS:       -1,
		firstFragmentMS:       -1,
		ttsFirstAudioMS:       -1,
		firstPlaybackQueuedMS: -1,
	}
}

func (m *turnMetrics) elapsedMS() int64 {
	return time.Since(m.asrFinalAt).Milliseconds()
}

func (m *turnMetrics) markLLMFirstToken() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.llmFirstTokenMS >= 0 {
		return false
	}
	m.llmFirstTokenMS = m.elapsedMS()
	return true
}

func (m *turnMetrics) markFirstFragment() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.firstFragmentMS >= 0 {
		return false
	}
	m.firstFragmentMS = m.elapsedMS()
	return true
}

func (m *turnMetrics) markTTSFirstAudio() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ttsFirstAudioMS >= 0 {
		return false
	}
	m.ttsFirstAudioMS = m.elapsedMS()
	return true
}

func (m *turnMetrics) markFirstPlaybackQueued() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.firstPlaybackQueuedMS >= 0 {
		return false
	}
	m.firstPlaybackQueuedMS = m.elapsedMS()
	return true
}

func (m *turnMetrics) addTTSChunk(size int) (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ttsChunks++
	m.ttsBytes += size
	return m.ttsChunks, m.ttsBytes
}

func (m *turnMetrics) addPlaybackSend(size int) (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.playbackSends++
	m.playbackBytes += size
	return m.playbackSends, m.playbackBytes
}

func (m *turnMetrics) logSummary(logger *slog.Logger, sessionID string) {
	m.mu.Lock()
	llmFirstTokenMS := m.llmFirstTokenMS
	firstFragmentMS := m.firstFragmentMS
	ttsFirstAudioMS := m.ttsFirstAudioMS
	firstPlaybackQueuedMS := m.firstPlaybackQueuedMS
	ttsChunks := m.ttsChunks
	ttsBytes := m.ttsBytes
	playbackSends := m.playbackSends
	playbackBytes := m.playbackBytes
	m.mu.Unlock()

	logger.Info("turn latency summary",
		"session", sessionID,
		"turn_id", m.id,
		"elapsed_ms", m.elapsedMS(),
		"asr_final_to_llm_first_token_ms", llmFirstTokenMS,
		"asr_final_to_first_tts_fragment_ms", firstFragmentMS,
		"asr_final_to_tts_first_audio_ms", ttsFirstAudioMS,
		"asr_final_to_playback_queued_ms", firstPlaybackQueuedMS,
		"final_asr_to_llm_first_token_ms", llmFirstTokenMS,
		"final_asr_to_first_tts_fragment_ms", firstFragmentMS,
		"final_asr_to_tts_first_audio_ms", ttsFirstAudioMS,
		"final_asr_to_playback_queued_ms", firstPlaybackQueuedMS,
		"tts_chunks", ttsChunks,
		"tts_bytes", ttsBytes,
		"playback_sends", playbackSends,
		"playback_bytes", playbackBytes,
	)
}

func copyBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func fmtPCM16OddLength(length int) error {
	return fmt.Errorf("pcm16 chunk has odd byte length %d", length)
}

func previewText(text string, maxRunes int) string {
	text = strings.TrimSpace(text)
	if maxRunes <= 0 {
		return text
	}
	runes := []rune(text)
	if len(runes) <= maxRunes {
		return text
	}
	return string(runes[:maxRunes]) + "..."
}

func trimHistory(history []llm.Message, historyTurns int) []llm.Message {
	if len(history) == 0 || historyTurns <= 0 {
		return append([]llm.Message(nil), history...)
	}

	start := 0
	trimmed := make([]llm.Message, 0, len(history))
	if history[0].Role == llm.RoleSystem {
		trimmed = append(trimmed, history[0])
		start = 1
	}

	cut := start
	turns := 0
	for i := len(history) - 1; i >= start; i-- {
		if history[i].Role != llm.RoleUser {
			continue
		}
		turns++
		cut = i
		if turns >= historyTurns {
			break
		}
	}

	trimmed = append(trimmed, history[cut:]...)
	return trimmed
}

func shouldFlushTTS(text string, maxChars, minChars int) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}

	runes := []rune(text)
	if maxChars > 0 && len(runes) >= maxChars {
		return true
	}

	last := runes[len(runes)-1]
	switch last {
	case '。', '！', '？', '.', '!', '?':
		return true
	case '，', ',', '；', ';', '：', ':':
		if minChars <= 0 {
			return true
		}
		return len(runes) >= minChars
	}
	return false
}
