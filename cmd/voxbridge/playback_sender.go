package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"voxbridge/internal/freeswitch"
)

const playbackFileTTL = 2 * time.Minute
const playbackSampleRate = 16000

// freeSwitchAudioSender is the fallback playback path for uuid_broadcast mode.
type freeSwitchAudioSender struct {
	client     *freeswitch.Client
	uuid       string
	sampleRate int
	logger     *slog.Logger

	mu    sync.Mutex
	seq   int64
	files map[string]struct{}
}

func newFreeSwitchAudioSender(client *freeswitch.Client, uuid string, sampleRate int, logger *slog.Logger) *freeSwitchAudioSender {
	if logger == nil {
		logger = slog.Default()
	}
	return &freeSwitchAudioSender{
		client:     client,
		uuid:       strings.TrimSpace(uuid),
		sampleRate: sampleRate,
		logger:     logger,
		files:      make(map[string]struct{}),
	}
}

func (s *freeSwitchAudioSender) SendPCM(ctx context.Context, pcm []byte) error {
	if len(pcm) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	file, seq, err := s.writePlaybackFile(pcm)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		s.removePlaybackFile(file)
		return err
	}

	commandCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.client.PlayAudio(commandCtx, s.uuid, file); err != nil {
		s.removePlaybackFile(file)
		return err
	}

	s.logger.Info("freeswitch playback queued",
		"uuid", s.uuid,
		"seq", seq,
		"file", file,
		"bytes", len(pcm),
		"sample_rate", s.sampleRate,
	)
	go s.removePlaybackFileAfter(file, playbackFileTTL)
	return nil
}

func (s *freeSwitchAudioSender) ClearQueued() {
	commandCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.client.BreakAudio(commandCtx, s.uuid); err != nil {
		s.logger.Warn("freeswitch playback break failed", "uuid", s.uuid, "error", err)
	} else {
		s.logger.Info("freeswitch playback break sent", "uuid", s.uuid)
	}
	s.removeAllPlaybackFiles()
}

func (s *freeSwitchAudioSender) writePlaybackFile(pcm []byte) (string, int64, error) {
	playbackPCM, err := pcm16ForPlayback(pcm, s.sampleRate)
	if err != nil {
		return "", 0, err
	}

	safeUUID := safeFilePart(s.uuid)
	if safeUUID == "" {
		safeUUID = "unknown"
	}
	file, err := os.CreateTemp("", "voxbridge-"+safeUUID+"-*.wav")
	if err != nil {
		return "", 0, fmt.Errorf("create playback file: %w", err)
	}
	path := file.Name()
	if err := writePCM16WAV(file, playbackPCM, playbackSampleRate); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", 0, fmt.Errorf("write playback file: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", 0, fmt.Errorf("close playback file: %w", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		_ = os.Remove(path)
		return "", 0, fmt.Errorf("chmod playback file: %w", err)
	}

	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.files[path] = struct{}{}
	s.mu.Unlock()

	return path, seq, nil
}

func pcm16ForPlayback(pcm []byte, sampleRate int) ([]byte, error) {
	if len(pcm)%2 != 0 {
		return nil, fmt.Errorf("pcm16 data has odd byte length %d", len(pcm))
	}
	if sampleRate <= 0 {
		sampleRate = 16000
	}
	switch sampleRate {
	case playbackSampleRate:
		return pcm, nil
	default:
		return nil, fmt.Errorf("unsupported playback input sample rate %d", sampleRate)
	}
}

func writePCM16WAV(w io.Writer, pcm []byte, sampleRate int) error {
	if len(pcm)%2 != 0 {
		return fmt.Errorf("pcm16 data has odd byte length %d", len(pcm))
	}
	if sampleRate <= 0 {
		sampleRate = 16000
	}

	const (
		channels      = 1
		bitsPerSample = 16
		blockAlign    = channels * bitsPerSample / 8
		audioFormat   = 1
	)
	dataSize := uint32(len(pcm))
	byteRate := uint32(sampleRate * blockAlign)

	if _, err := io.WriteString(w, "RIFF"); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(36)+dataSize); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "WAVEfmt "); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(16)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(audioFormat)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(channels)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(sampleRate)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, byteRate); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(blockAlign)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(bitsPerSample)); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "data"); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, dataSize); err != nil {
		return err
	}
	_, err := w.Write(pcm)
	return err
}

func (s *freeSwitchAudioSender) removePlaybackFileAfter(file string, ttl time.Duration) {
	timer := time.NewTimer(ttl)
	defer timer.Stop()
	<-timer.C
	s.removePlaybackFile(file)
}

func (s *freeSwitchAudioSender) removeAllPlaybackFiles() {
	s.mu.Lock()
	files := make([]string, 0, len(s.files))
	for file := range s.files {
		files = append(files, file)
		delete(s.files, file)
	}
	s.mu.Unlock()

	for _, file := range files {
		_ = os.Remove(file)
	}
}

func (s *freeSwitchAudioSender) removePlaybackFile(file string) {
	s.mu.Lock()
	if _, ok := s.files[file]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.files, file)
	s.mu.Unlock()

	if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
		s.logger.Warn("remove playback file failed", "uuid", s.uuid, "file", file, "error", err)
	}
}

func safeFilePart(value string) string {
	value = filepath.Base(strings.TrimSpace(value))
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
