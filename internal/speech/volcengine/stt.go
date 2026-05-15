package volcengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"voxbridge/internal/speech"
)

type STTProvider struct {
	cfg STTConfig
}

func NewSTTProvider(cfg STTConfig) (*STTProvider, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &STTProvider{cfg: cfg}, nil
}

func (p *STTProvider) Start(ctx context.Context, sessionID string) (speech.STTSession, error) {
	if sessionID == "" {
		sessionID = newRequestID()
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()

	endpoint := p.cfg.Endpoint
	if p.cfg.isAsync() {
		endpoint = withRequestID(endpoint, sessionID)
	}
	logger := p.cfg.Logger.With("provider", "volcengine", "component", "stt", "session", sessionID)
	logger.Info("volcengine stt connecting",
		"endpoint", endpoint,
		"async", p.cfg.isAsync(),
		"sample_rate", p.cfg.Audio.Rate,
		"channels", p.cfg.Audio.Channel,
		"chunk_size", p.cfg.ChunkSize,
	)
	conn, resp, err := p.cfg.dialer.DialContext(ctx, endpoint, authHeaders(p.cfg.AppID, p.cfg.Token, p.cfg.AccessKey, p.cfg.ResourceID, sessionID))
	if err != nil {
		logger.Warn("volcengine stt connect failed", "error", err)
		return nil, websocketDialError("connect volcengine STT websocket", resp, err)
	}
	logger.Info("volcengine stt connected", "endpoint", endpoint)

	s := &sttSession{
		cfg:       p.cfg,
		conn:      conn,
		events:    make(chan speech.RecognitionEvent, 16),
		done:      make(chan struct{}),
		sessionID: sessionID,
		endpoint:  endpoint,
		logger:    logger,

		emittedFinals: make(map[string]struct{}),
	}
	if err := s.sendInitialRequest(sessionID); err != nil {
		conn.Close()
		logger.Warn("volcengine stt init failed", "error", err)
		return nil, err
	}
	go s.readLoop()
	return s, nil
}

type sttSession struct {
	cfg    STTConfig
	conn   *websocket.Conn
	events chan speech.RecognitionEvent
	done   chan struct{}

	sessionID string
	endpoint  string
	logger    *slog.Logger

	audioFrames    atomic.Int64
	audioBytes     atomic.Int64
	responseFrames atomic.Int64
	partialEvents  atomic.Int64
	finalEvents    atomic.Int64
	emittedFinals  map[string]struct{}

	mu     sync.Mutex
	seq    int32
	closed bool
	err    error
}

func (s *sttSession) Events() <-chan speech.RecognitionEvent {
	return s.events
}

func (s *sttSession) WritePCM(ctx context.Context, pcm []byte) error {
	if len(pcm) == 0 {
		return nil
	}
	for len(pcm) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := len(pcm)
		if s.cfg.ChunkSize > 0 && n > s.cfg.ChunkSize {
			n = s.cfg.ChunkSize
		}
		if err := s.writePCMFrame(ctx, pcm[:n]); err != nil {
			return err
		}
		pcm = pcm[n:]
	}
	return nil
}

func (s *sttSession) writePCMFrame(ctx context.Context, pcm []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("volcengine STT session is closed")
	}
	var seq *int32
	flags := byte(messageFlagNone)
	if !s.cfg.isAsync() {
		s.seq++
		seq = &s.seq
		flags = messageFlagSequence
	}
	payload, err := buildFrame(messageTypeAudioOnlyRequest, flags, serializationNone, compressionGzip, seq, pcm)
	if err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = s.conn.SetWriteDeadline(deadline)
	} else {
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.cfg.Timeout))
	}
	if err := s.conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		return fmt.Errorf("write volcengine STT audio frame: %w", err)
	}
	frames := s.audioFrames.Add(1)
	bytes := s.audioBytes.Add(int64(len(pcm)))
	if frames <= 5 || frames%50 == 0 {
		sequence := int32(0)
		if seq != nil {
			sequence = *seq
		}
		s.logger.Info("volcengine stt audio sent",
			"frames", frames,
			"bytes", bytes,
			"last_bytes", len(pcm),
			"sequence", sequence,
		)
	}
	return nil
}

func (s *sttSession) Close() error {
	s.logger.Info("volcengine stt closing",
		"audio_frames", s.audioFrames.Load(),
		"audio_bytes", s.audioBytes.Load(),
		"responses", s.responseFrames.Load(),
		"partials", s.partialEvents.Load(),
		"finals", s.finalEvents.Load(),
	)
	s.mu.Lock()
	if s.closed {
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.closed = true
	var seq *int32
	flags := byte(messageFlagLast)
	if !s.cfg.isAsync() {
		s.seq = -absInt32(s.seq + 1)
		seqValue := s.seq
		seq = &seqValue
		flags = messageFlagSequenceEnd
	}
	endFrame, frameErr := buildFrame(messageTypeAudioOnlyRequest, flags, serializationNone, compressionGzip, seq, nil)
	if frameErr == nil {
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.cfg.Timeout))
		frameErr = s.conn.WriteMessage(websocket.BinaryMessage, endFrame)
	}
	closeErr := s.conn.Close()
	s.mu.Unlock()

	select {
	case <-s.done:
	case <-time.After(s.cfg.Timeout):
	}
	if frameErr != nil {
		s.logger.Warn("volcengine stt close frame failed", "error", frameErr)
		return frameErr
	}
	if closeErr != nil {
		s.logger.Warn("volcengine stt websocket close failed", "error", closeErr)
		return closeErr
	}
	if s.err != nil {
		s.logger.Warn("volcengine stt closed with error",
			"error", s.err,
			"audio_frames", s.audioFrames.Load(),
			"audio_bytes", s.audioBytes.Load(),
			"responses", s.responseFrames.Load(),
			"partials", s.partialEvents.Load(),
			"finals", s.finalEvents.Load(),
		)
	} else {
		s.logger.Info("volcengine stt closed",
			"audio_frames", s.audioFrames.Load(),
			"audio_bytes", s.audioBytes.Load(),
			"responses", s.responseFrames.Load(),
			"partials", s.partialEvents.Load(),
			"finals", s.finalEvents.Load(),
		)
	}
	return s.err
}

func (s *sttSession) sendInitialRequest(sessionID string) error {
	if s.cfg.isAsync() {
		return s.sendAsyncInitialRequest(sessionID)
	}
	req := map[string]any{
		"user": map[string]any{
			"uid": sessionID,
		},
		"audio": map[string]any{
			"format":  s.cfg.Audio.Format,
			"codec":   valueOr(s.cfg.Audio.Codec, "raw"),
			"rate":    valueOrInt(s.cfg.Audio.Rate, defaultSampleRate),
			"bits":    valueOrInt(s.cfg.Audio.Bits, defaultBits),
			"channel": valueOrInt(s.cfg.Audio.Channel, defaultChannels),
		},
		"request": map[string]any{
			"reqid":           sessionID,
			"workflow":        s.cfg.Workflow,
			"show_utterances": true,
			"result_type":     "single",
			"sequence":        1,
		},
	}
	if s.cfg.Audio.Language != "" {
		req["audio"].(map[string]any)["language"] = s.cfg.Audio.Language
	}
	if s.cfg.Cluster != "" {
		req["cluster"] = s.cfg.Cluster
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	frame, err := buildFrame(messageTypeFullClientRequest, messageFlagSequence, serializationJSON, compressionGzip, int32Ptr(1), payload)
	if err != nil {
		return err
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(s.cfg.Timeout))
	if err := s.conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return fmt.Errorf("write volcengine STT initial request: %w", err)
	}
	s.logger.Info("volcengine stt init sent",
		"mode", "streaming",
		"payload_bytes", len(payload),
		"frame_bytes", len(frame),
		"sample_rate", valueOrInt(s.cfg.Audio.Rate, defaultSampleRate),
		"channels", valueOrInt(s.cfg.Audio.Channel, defaultChannels),
		"workflow", s.cfg.Workflow,
	)
	return nil
}

func (s *sttSession) sendAsyncInitialRequest(sessionID string) error {
	req := s.asyncInitialRequest(sessionID)
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	frame, err := buildFrame(messageTypeFullClientRequest, messageFlagNone, serializationJSON, compressionGzip, nil, payload)
	if err != nil {
		return err
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(s.cfg.Timeout))
	if err := s.conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return fmt.Errorf("write volcengine async STT initial request: %w", err)
	}
	s.logger.Info("volcengine stt init sent",
		"mode", "async",
		"payload_bytes", len(payload),
		"frame_bytes", len(frame),
		"sample_rate", valueOrInt(s.cfg.Audio.Rate, defaultSampleRate),
		"channels", valueOrInt(s.cfg.Audio.Channel, defaultChannels),
		"model", "bigmodel",
		"end_window_size", s.cfg.AsyncEndWindowSize,
		"force_to_speech_time", s.cfg.AsyncForceToSpeechTime,
	)
	return nil
}

func (s *sttSession) asyncInitialRequest(sessionID string) map[string]any {
	req := map[string]any{
		"user": map[string]any{
			"uid":         sessionID,
			"did":         "voxbridge",
			"platform":    "freeswitch",
			"sdk_version": "0.1.0",
			"app_version": "0.1.0",
		},
		"audio": map[string]any{
			"format":  s.cfg.Audio.Format,
			"codec":   valueOr(s.cfg.Audio.Codec, "raw"),
			"rate":    valueOrInt(s.cfg.Audio.Rate, defaultSampleRate),
			"bits":    valueOrInt(s.cfg.Audio.Bits, defaultBits),
			"channel": valueOrInt(s.cfg.Audio.Channel, defaultChannels),
		},
		"request": map[string]any{
			"model_name":           "bigmodel",
			"model_version":        "400",
			"enable_itn":           true,
			"enable_punc":          true,
			"enable_ddc":           false,
			"show_utterances":      true,
			"result_type":          "full",
			"end_window_size":      s.cfg.AsyncEndWindowSize,
			"force_to_speech_time": s.cfg.AsyncForceToSpeechTime,
		},
	}
	if s.cfg.Audio.Language != "" {
		req["audio"].(map[string]any)["language"] = s.cfg.Audio.Language
	}
	return req
}

func (s *sttSession) readLoop() {
	defer close(s.done)
	defer close(s.events)
	s.logger.Info("volcengine stt read loop started")
	for {
		mt, data, err := s.conn.ReadMessage()
		if err != nil {
			if s.setErr(err) {
				s.logger.Warn("volcengine stt read failed", "error", err)
			}
			return
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		var f frame
		var parseErr error
		if s.cfg.isAsync() {
			f, parseErr = parseAsyncASRFrame(data)
		} else {
			f, parseErr = parseFrame(data)
		}
		if parseErr != nil {
			if s.setErr(parseErr) {
				s.logger.Warn("volcengine stt response parse failed", "error", parseErr, "frame_bytes", len(data))
			}
			return
		}
		frames := s.responseFrames.Add(1)
		if frames <= 5 || frames%20 == 0 {
			s.logger.Info("volcengine stt response frame",
				"frames", frames,
				"message_type", f.MessageType,
				"flags", f.MessageFlags,
				"sequence", f.Sequence,
				"payload_bytes", len(f.Payload),
			)
		}
		if f.MessageType == messageTypeError {
			err := fmt.Errorf("volcengine STT error %d: %s", f.ErrorCode, string(f.Payload))
			if s.setErr(err) {
				s.logger.Warn("volcengine stt error frame", "code", f.ErrorCode, "payload", textPreview(string(f.Payload), 200))
			}
			return
		}
		events, err := parseRecognitionEvents(f.Payload, f.MessageFlags&messageFlagLast != 0 || f.Sequence < 0)
		if err != nil {
			if s.setErr(err) {
				s.logger.Warn("volcengine stt recognition parse failed", "error", err, "payload", textPreview(string(f.Payload), 200))
			}
			return
		}
		receivedAt := time.Now()
		audioBytes := s.audioBytes.Load()
		for _, ev := range events {
			ev.ReceivedAt = receivedAt
			ev.FrameIndex = frames
			ev.AudioBytes = audioBytes
			if !s.shouldEmitRecognitionEvent(ev) {
				continue
			}
			s.logRecognitionEvent(ev)
			s.events <- ev
		}
	}
}

func parseAsyncASRFrame(data []byte) (frame, error) {
	if len(data) < 12 {
		return frame{}, fmt.Errorf("volcengine async ASR frame too short: %d bytes", len(data))
	}
	if got := data[0] >> 4; got != protocolVersion {
		return frame{}, fmt.Errorf("unsupported volcengine protocol version %d", got)
	}
	headerSize := int(data[0]&0x0f) * 4
	if headerSize < 4 || len(data) < headerSize+8 {
		return frame{}, fmt.Errorf("invalid volcengine async header size %d for %d-byte frame", headerSize, len(data))
	}
	f := frame{
		MessageType:   data[1] >> 4,
		MessageFlags:  data[1] & 0x0f,
		Serialization: data[2] >> 4,
		Compression:   data[2] & 0x0f,
	}
	pos := headerSize
	if f.MessageType == messageTypeError {
		f.ErrorCode = binaryBigEndianUint32(data[pos : pos+4])
		pos += 4
	} else if f.MessageFlags&messageFlagSequence != 0 {
		f.Sequence = int32(binaryBigEndianUint32(data[pos : pos+4]))
		pos += 4
	}
	payloadSize := int(binaryBigEndianUint32(data[pos : pos+4]))
	pos += 4
	if payloadSize < 0 || len(data)-pos < payloadSize {
		return frame{}, fmt.Errorf("invalid volcengine async payload size %d for remaining %d bytes", payloadSize, len(data)-pos)
	}
	f.Payload = data[pos : pos+payloadSize]
	if f.Compression == compressionGzip && len(f.Payload) > 0 {
		payload, err := gunzipBytes(f.Payload)
		if err != nil {
			return frame{}, err
		}
		f.Payload = payload
	}
	return f, nil
}

func binaryBigEndianUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func withRequestID(endpoint, requestID string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}
	q := u.Query()
	if strings.TrimSpace(q.Get("request_id")) == "" {
		q.Set("request_id", requestID)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *sttSession) setErr(err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || errors.Is(err, http.ErrServerClosed) || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		return false
	}
	s.err = err
	return true
}

func (s *sttSession) logRecognitionEvent(ev speech.RecognitionEvent) {
	kind := recognitionKindName(ev.Kind)
	var count int64
	shouldLog := true
	if ev.Kind == speech.RecognitionFinal {
		count = s.finalEvents.Add(1)
	} else {
		count = s.partialEvents.Add(1)
		shouldLog = count <= 10 || count%10 == 0
	}
	if !shouldLog {
		return
	}
	s.logger.Info("volcengine stt recognition event",
		"kind", kind,
		"index", count,
		"received_at", ev.ReceivedAt,
		"frame_index", ev.FrameIndex,
		"audio_bytes", ev.AudioBytes,
		"chars", len([]rune(ev.Text)),
		"text", textPreview(ev.Text, 120),
	)
}

func (s *sttSession) shouldEmitRecognitionEvent(ev speech.RecognitionEvent) bool {
	if ev.Kind != speech.RecognitionFinal {
		return true
	}
	text := strings.TrimSpace(ev.Text)
	if text == "" {
		return false
	}
	if _, ok := s.emittedFinals[text]; ok {
		s.logger.Info("volcengine stt duplicate final skipped",
			"chars", len([]rune(text)),
			"text", textPreview(text, 120),
		)
		return false
	}
	s.emittedFinals[text] = struct{}{}
	return true
}

func recognitionKindName(kind speech.RecognitionKind) string {
	switch kind {
	case speech.RecognitionFinal:
		return "final"
	case speech.RecognitionPartial:
		return "partial"
	default:
		return "unknown"
	}
}

type sttResult struct {
	Result struct {
		Text       string `json:"text"`
		Utterances []struct {
			Text     string `json:"text"`
			Definite bool   `json:"definite"`
		} `json:"utterances"`
	} `json:"result"`
	Text       string `json:"text"`
	Definite   bool   `json:"definite"`
	IsFinal    bool   `json:"is_final"`
	Utterances []struct {
		Text     string `json:"text"`
		Definite bool   `json:"definite"`
	} `json:"utterances"`
}

func parseRecognitionEvents(payload []byte, frameFinal bool) ([]speech.RecognitionEvent, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var result sttResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, fmt.Errorf("parse volcengine STT result: %w", err)
	}

	var events []speech.RecognitionEvent
	add := func(text string, final bool) {
		if text == "" {
			return
		}
		kind := speech.RecognitionPartial
		if final {
			kind = speech.RecognitionFinal
		}
		events = append(events, speech.RecognitionEvent{Kind: kind, Text: text})
	}
	for _, u := range result.Result.Utterances {
		add(u.Text, u.Definite || frameFinal)
	}
	for _, u := range result.Utterances {
		add(u.Text, u.Definite || frameFinal)
	}
	if len(events) == 0 {
		add(valueOr(result.Result.Text, result.Text), result.Definite || result.IsFinal || frameFinal)
	}
	return events, nil
}
