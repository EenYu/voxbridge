package media

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"voxbridge/internal/dialog"
	"voxbridge/internal/playback"
)

const (
	MediaPath           = "/fs/media"
	MediaCompatiblePath = MediaPath
)

type ConnectionInfo struct {
	CallUUID string
	Metadata map[string]string
}

type DialogSession interface {
	WritePCM(ctx context.Context, pcm []byte) error
	Close() error
}

type SessionFactory func(ctx context.Context, info ConnectionInfo, sender dialog.AudioSender) (DialogSession, error)

type Handler struct {
	Factory SessionFactory

	Upgrader websocket.Upgrader
	Logger   *slog.Logger

	PlaybackMode        playback.Mode
	SampleRate          int
	Channels            int
	BytesPerSample      int
	TargetChunkDuration time.Duration
}

func NewHandler(factory SessionFactory) *Handler {
	return &Handler{
		Factory: factory,
		Upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		PlaybackMode:        playback.ModeWSBinary,
		SampleRate:          DefaultSampleRate,
		Channels:            DefaultChannels,
		BytesPerSample:      DefaultBytesPerSample,
		TargetChunkDuration: DefaultChunkDuration,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.Factory == nil {
		http.Error(w, "media session factory is not configured", http.StatusInternalServerError)
		return
	}

	upgrader := h.Upgrader
	if upgrader.CheckOrigin == nil {
		upgrader.CheckOrigin = func(*http.Request) bool { return true }
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	logger := h.logger()

	info := ConnectionInfo{
		CallUUID: callUUIDFromQuery(r),
		Metadata: map[string]string{
			"remote_addr": r.RemoteAddr,
		},
	}
	stats := newInboundAudioStats()
	logger.Info("media websocket connected", "uuid", info.CallUUID, "remote", r.RemoteAddr)
	defer func() {
		logger.Info(
			"media websocket closed",
			"uuid", info.CallUUID,
			"remote", r.RemoteAddr,
			"frames", stats.frames,
			"bytes", stats.bytes,
			"rms", stats.rms(),
			"peak", stats.peak,
		)
	}()

	sender := newWebSocketAudioSender(conn, h.sampleRate(), h.chunkSize(), h.playbackMode())
	var session DialogSession
	defer func() {
		if session != nil {
			_ = session.Close()
		}
	}()

	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}

		switch messageType {
		case websocket.BinaryMessage:
			stats.observe(payload)
			if stats.shouldLog(time.Now()) {
				logger.Info(
					"media inbound audio",
					"uuid", info.CallUUID,
					"frames", stats.frames,
					"bytes", stats.bytes,
					"last_bytes", len(payload),
					"rms", stats.rms(),
					"peak", stats.peak,
				)
			}
			if session == nil {
				session, err = h.Factory(ctx, cloneConnectionInfo(info), sender)
				if err != nil {
					logger.Warn("media session creation failed", "uuid", info.CallUUID, "error", err)
					writeClose(conn, websocket.CloseInternalServerErr, "failed to create media session")
					return
				}
			}
			if err := session.WritePCM(ctx, payload); err != nil {
				logger.Warn("media pcm forward failed", "uuid", info.CallUUID, "error", err)
				writeClose(conn, websocket.CloseInternalServerErr, "failed to forward pcm")
				return
			}
		case websocket.TextMessage:
			eventInfo := parseTextEvent(payload)
			mergeMetadata(info.Metadata, eventInfo.Metadata)
			if info.CallUUID == "" {
				info.CallUUID = eventInfo.CallUUID
			}
			logger.Debug("media text event received", "uuid", info.CallUUID, "metadata_keys", len(eventInfo.Metadata), "should_start", eventInfo.ShouldStart)
			if session == nil && eventInfo.ShouldStart {
				session, err = h.Factory(ctx, cloneConnectionInfo(info), sender)
				if err != nil {
					logger.Warn("media session creation failed", "uuid", info.CallUUID, "error", err)
					writeClose(conn, websocket.CloseInternalServerErr, "failed to create media session")
					return
				}
			}
		case websocket.CloseMessage:
			return
		}
	}
}

func (h *Handler) sampleRate() int {
	if h.SampleRate <= 0 {
		return DefaultSampleRate
	}
	return h.SampleRate
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

func (h *Handler) chunkSize() int {
	if h.playbackMode() == playback.ModeUUIDBroadcast {
		return ChunkSizeBytes(h.SampleRate, h.Channels, h.BytesPerSample, h.TargetChunkDuration)
	}
	return BinaryChunkSizeBytes(h.SampleRate, h.Channels, h.BytesPerSample, h.TargetChunkDuration)
}

func (h *Handler) playbackMode() playback.Mode {
	mode := playback.Normalize(h.PlaybackMode)
	if !playback.IsValid(mode) {
		return playback.ModeWSBinary
	}
	return mode
}

func callUUIDFromQuery(r *http.Request) string {
	for _, key := range []string{"uuid", "call_uuid", "callUUID", "unique_id", "Unique-ID"} {
		if value := strings.TrimSpace(r.URL.Query().Get(key)); value != "" {
			return value
		}
	}
	return ""
}

type textEventInfo struct {
	CallUUID    string
	Metadata    map[string]string
	ShouldStart bool
}

func parseTextEvent(payload []byte) textEventInfo {
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return textEventInfo{Metadata: map[string]string{"text": string(payload)}}
	}

	info := textEventInfo{Metadata: make(map[string]string)}
	flattenMetadata(info.Metadata, "", raw)
	info.CallUUID = firstMetadataValue(info.Metadata,
		"uuid",
		"call_uuid",
		"callUUID",
		"unique_id",
		"Unique-ID",
		"metadata.uuid",
		"metadata.call_uuid",
		"metadata.callUUID",
		"metadata.unique_id",
		"metadata.Unique-ID",
	)
	eventType := strings.ToLower(firstMetadataValue(info.Metadata, "event", "type", "name"))
	info.ShouldStart = info.CallUUID != "" || eventType == "start" || eventType == "metadata" || eventType == "connect" || eventType == "connected"
	return info
}

func flattenMetadata(out map[string]string, prefix string, value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			nextKey := key
			if prefix != "" {
				nextKey = prefix + "." + key
			}
			flattenMetadata(out, nextKey, nested)
		}
	case string:
		out[prefix] = typed
	case float64, bool, nil:
		encoded, _ := json.Marshal(typed)
		out[prefix] = string(encoded)
	default:
		encoded, err := json.Marshal(typed)
		if err == nil {
			out[prefix] = string(encoded)
		}
	}
}

func mergeMetadata(dst, src map[string]string) {
	for key, value := range src {
		if key != "" && value != "" {
			dst[key] = value
		}
	}
}

func firstMetadataValue(metadata map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(metadata[key]); value != "" {
			return value
		}
	}
	return ""
}

func cloneConnectionInfo(info ConnectionInfo) ConnectionInfo {
	metadata := make(map[string]string, len(info.Metadata))
	for key, value := range info.Metadata {
		metadata[key] = value
	}
	info.Metadata = metadata
	return info
}

func writeClose(conn *websocket.Conn, code int, reason string) {
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(code, reason),
		time.Now().Add(time.Second),
	)
}

type inboundAudioStats struct {
	bytes       int64
	frames      int64
	samples     int64
	sumSquares  float64
	peak        int
	logInterval time.Duration
	lastLog     time.Time
}

func newInboundAudioStats() *inboundAudioStats {
	return &inboundAudioStats{
		logInterval: time.Second,
		lastLog:     time.Now(),
	}
}

func (s *inboundAudioStats) observe(pcm []byte) {
	if len(pcm) == 0 {
		return
	}
	s.frames++
	s.bytes += int64(len(pcm))
	for i := 0; i+1 < len(pcm); i += 2 {
		v := int(int16(uint16(pcm[i]) | uint16(pcm[i+1])<<8))
		abs := v
		if abs < 0 {
			abs = -abs
		}
		if abs > s.peak {
			s.peak = abs
		}
		s.samples++
		s.sumSquares += float64(v) * float64(v)
	}
}

func (s *inboundAudioStats) rms() int {
	if s.samples == 0 {
		return 0
	}
	return int(math.Round(math.Sqrt(s.sumSquares / float64(s.samples))))
}

func (s *inboundAudioStats) shouldLog(now time.Time) bool {
	if s.frames == 1 || now.Sub(s.lastLog) >= s.logInterval {
		s.lastLog = now
		return true
	}
	return false
}

type webSocketAudioSender struct {
	conn         *websocket.Conn
	sampleRate   int
	chunkSize    int
	playbackMode playback.Mode
	writeMu      sync.Mutex
	paceMu       sync.Mutex
	nextWriteAt  time.Time
}

func newWebSocketAudioSender(conn *websocket.Conn, sampleRate, chunkSize int, playbackMode playback.Mode) *webSocketAudioSender {
	return &webSocketAudioSender{
		conn:         conn,
		sampleRate:   sampleRate,
		chunkSize:    chunkSize,
		playbackMode: playbackMode,
	}
}

func (s *webSocketAudioSender) SendPCM(ctx context.Context, pcm []byte) error {
	if len(pcm) == 0 {
		return nil
	}
	if s.chunkSize <= 0 {
		s.chunkSize = ChunkSizeBytes(s.sampleRate, DefaultChannels, DefaultBytesPerSample, DefaultChunkDuration)
	}
	paceWrites := playback.Normalize(s.playbackMode) == playback.ModeWSBinary

	for start := 0; start < len(pcm); start += s.chunkSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + s.chunkSize
		if end > len(pcm) {
			end = len(pcm)
		}
		if paceWrites {
			if err := s.waitForWriteSlot(ctx, end-start); err != nil {
				return err
			}
		}
		if err := s.writePCMChunk(pcm[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *webSocketAudioSender) ClearQueued() {
	s.resetPacing()
	_ = s.writeJSON(newClearAudioMessage())
}

func (s *webSocketAudioSender) waitForWriteSlot(ctx context.Context, bytes int) error {
	delay := s.reserveWriteDelay(time.Now(), bytes)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *webSocketAudioSender) reserveWriteDelay(now time.Time, bytes int) time.Duration {
	duration := pcmChunkDuration(s.sampleRate, bytes)

	s.paceMu.Lock()
	defer s.paceMu.Unlock()

	target := s.nextWriteAt
	if target.IsZero() || !target.After(now) {
		target = now
	}
	s.nextWriteAt = target.Add(duration)
	return target.Sub(now)
}

func (s *webSocketAudioSender) resetPacing() {
	s.paceMu.Lock()
	s.nextWriteAt = time.Time{}
	s.paceMu.Unlock()
}

func (s *webSocketAudioSender) writeJSON(message any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.conn == nil {
		return errors.New("websocket connection is nil")
	}
	return s.conn.WriteJSON(message)
}

func (s *webSocketAudioSender) writePCMChunk(pcm []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.conn == nil {
		return errors.New("websocket connection is nil")
	}
	if playback.Normalize(s.playbackMode) == playback.ModeUUIDBroadcast {
		return s.conn.WriteJSON(newStreamAudioMessage(s.sampleRate, pcm))
	}
	return s.conn.WriteMessage(websocket.BinaryMessage, pcm)
}

func pcmChunkDuration(sampleRate, bytes int) time.Duration {
	if sampleRate <= 0 {
		sampleRate = DefaultSampleRate
	}
	if bytes <= 0 {
		return 0
	}

	bytesPerSecond := sampleRate * DefaultChannels * DefaultBytesPerSample
	if bytesPerSecond <= 0 {
		return 0
	}
	return time.Duration(int64(bytes) * int64(time.Second) / int64(bytesPerSecond))
}
