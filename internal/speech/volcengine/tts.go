package volcengine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

const (
	ttsFlagWithEvent = 0x04

	ttsEventStartConnection  = 1
	ttsEventFinishConnection = 2
	ttsEventConnectionStart  = 50
	ttsEventConnectionFail   = 51

	ttsEventStartSession  = 100
	ttsEventFinishSession = 102
	ttsEventSessionStart  = 150
	ttsEventSessionFinish = 152
	ttsEventSessionFail   = 153

	ttsEventTaskRequest   = 200
	ttsEventSentenceStart = 350
	ttsEventSentenceEnd   = 351
	ttsEventResponse      = 352
)

type TTSProvider struct {
	cfg TTSConfig
}

func NewTTSProvider(cfg TTSConfig) (*TTSProvider, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &TTSProvider{cfg: cfg}, nil
}

func (p *TTSProvider) Synthesize(ctx context.Context, text string) (<-chan []byte, <-chan error) {
	audioCh := make(chan []byte, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(audioCh)
		defer close(errCh)
		if text == "" {
			return
		}
		if err := p.synthesize(ctx, text, audioCh); err != nil {
			errCh <- err
		}
	}()
	return audioCh, errCh
}

func (p *TTSProvider) synthesize(ctx context.Context, text string, audioCh chan<- []byte) error {
	requestID := newRequestID()
	logger := p.cfg.Logger.With("provider", "volcengine", "component", "tts", "request", requestID)
	logger.Info("volcengine tts connecting",
		"endpoint", p.cfg.Endpoint,
		"sample_rate", valueOrInt(p.cfg.Audio.Rate, defaultSampleRate),
		"voice_type", p.cfg.Audio.VoiceType,
		"chars", len([]rune(text)),
	)
	dialCtx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	connectStarted := time.Now()
	conn, resp, err := p.cfg.dialer.DialContext(dialCtx, p.cfg.Endpoint, authHeaders(p.cfg.AppID, p.cfg.Token, p.cfg.AccessKey, p.cfg.ResourceID, requestID))
	if err != nil {
		logger.Warn("volcengine tts connect failed", "error", err, "connect_ms", time.Since(connectStarted).Milliseconds())
		return websocketDialError("connect volcengine TTS websocket", resp, err)
	}
	defer conn.Close()
	logger.Info("volcengine tts connected",
		"endpoint", p.cfg.Endpoint,
		"connect_ms", time.Since(connectStarted).Milliseconds(),
	)

	startConnectionPayload := []byte("{}")
	if err := p.sendTTSEvent(ctx, conn, ttsEventStartConnection, "", startConnectionPayload, serializationNone); err != nil {
		return err
	}
	logTTSEventSent(logger, ttsEventStartConnection, "", startConnectionPayload, serializationNone)
	if err := p.expectTTSEvent(ctx, conn, ttsEventConnectionStart, logger); err != nil {
		return err
	}

	sessionID := newRequestID()
	startSessionPayload := p.ttsPayload(requestID, ttsEventStartSession, "", sessionID)
	sessionStartRequested := time.Now()
	if err := p.sendTTSEvent(ctx, conn, ttsEventStartSession, sessionID, startSessionPayload, serializationJSON); err != nil {
		return err
	}
	logTTSEventSent(logger, ttsEventStartSession, sessionID, startSessionPayload, serializationJSON)
	if err := p.expectTTSEvent(ctx, conn, ttsEventSessionStart, logger); err != nil {
		return err
	}
	logger.Info("volcengine tts session started",
		"session_id", sessionID,
		"session_start_ms", time.Since(sessionStartRequested).Milliseconds(),
	)
	taskPayload := p.ttsPayload(requestID, ttsEventTaskRequest, text, sessionID)
	if err := p.sendTTSEvent(ctx, conn, ttsEventTaskRequest, sessionID, taskPayload, serializationJSON); err != nil {
		return err
	}
	taskSentAt := time.Now()
	logTTSEventSent(logger, ttsEventTaskRequest, sessionID, taskPayload, serializationJSON)
	logger.Info("volcengine tts task sent",
		"session_id", sessionID,
		"chars", len([]rune(text)),
		"text", textPreview(text, 160),
	)
	finishSessionPayload := []byte("{}")
	if err := p.sendTTSEvent(ctx, conn, ttsEventFinishSession, sessionID, finishSessionPayload, serializationJSON); err != nil {
		return err
	}
	logTTSEventSent(logger, ttsEventFinishSession, sessionID, finishSessionPayload, serializationJSON)

	var responseFrames int64
	var audioChunks int64
	var audioBytes int64
	var firstAudioBytes int
	for {
		resp, err := p.readTTSResponse(ctx, conn)
		if err != nil {
			logger.Warn("volcengine tts read failed", "error", err)
			return err
		}
		responseFrames++
		if resp.messageType != messageTypeAudioResult || responseFrames <= 5 || responseFrames%20 == 0 {
			logTTSResponse(logger, resp, responseFrames)
		}
		switch resp.messageType {
		case messageTypeError:
			logger.Warn("volcengine tts error frame", "code", resp.errorCode, "payload", textPreview(string(resp.payload), 200))
			return fmt.Errorf("volcengine TTS error %d: %s", resp.errorCode, string(resp.payload))
		case messageTypeAudioResult:
			if resp.event == ttsEventResponse && len(resp.payload) > 0 {
				chunk := append([]byte(nil), resp.payload...)
				audioChunks++
				audioBytes += int64(len(chunk))
				if firstAudioBytes == 0 {
					firstAudioBytes = len(chunk)
					logger.Info("volcengine tts first audio",
						"session_id", sessionID,
						"task_to_first_audio_ms", time.Since(taskSentAt).Milliseconds(),
						"first_audio_bytes", firstAudioBytes,
						"response_frame", responseFrames,
					)
				}
				if audioChunks <= 3 || audioChunks%20 == 0 {
					logger.Info("volcengine tts audio chunk",
						"session_id", sessionID,
						"chunks", audioChunks,
						"bytes", audioBytes,
						"last_bytes", len(chunk),
					)
				}
				select {
				case audioCh <- chunk:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		case messageTypeFullServerResult:
			switch resp.event {
			case ttsEventSentenceStart, ttsEventSentenceEnd:
				continue
			case ttsEventSessionFinish:
				finishConnectionPayload := []byte("{}")
				if err := p.sendTTSEvent(ctx, conn, ttsEventFinishConnection, "", finishConnectionPayload, serializationJSON); err != nil {
					logger.Warn("volcengine tts finish connection failed", "error", err)
				} else {
					logTTSEventSent(logger, ttsEventFinishConnection, "", finishConnectionPayload, serializationJSON)
				}
				logger.Info("volcengine tts finished",
					"session_id", sessionID,
					"response_frames", responseFrames,
					"audio_chunks", audioChunks,
					"audio_bytes", audioBytes,
					"first_audio_bytes", firstAudioBytes,
				)
				return nil
			case ttsEventSessionFail, ttsEventConnectionFail:
				logger.Warn("volcengine tts failed event",
					"event", ttsEventName(resp.event),
					"event_id", resp.event,
					"meta", textPreview(resp.meta, 200),
					"payload", textPreview(string(resp.payload), 200),
				)
				return fmt.Errorf("volcengine TTS event %d failed: %s %s", resp.event, resp.meta, string(resp.payload))
			}
		}
	}
}

func (p *TTSProvider) ttsPayload(uid string, event int32, text, sessionID string) []byte {
	params := map[string]any{
		"speaker": p.cfg.Audio.VoiceType,
		"audio_params": map[string]any{
			"format":      valueOr(p.cfg.Audio.Encoding, p.cfg.Audio.Format),
			"sample_rate": valueOrInt(p.cfg.Audio.Rate, defaultSampleRate),
		},
	}
	if text != "" {
		params["text"] = text
	}
	if p.cfg.Audio.SpeechRate != 0 {
		params["speech_rate"] = p.cfg.Audio.SpeechRate
	}
	if p.cfg.Audio.PitchRate != 0 {
		params["pitch_rate"] = p.cfg.Audio.PitchRate
	}
	payload, _ := json.Marshal(map[string]any{
		"user":       map[string]any{"uid": uid},
		"event":      event,
		"namespace":  "BidirectionalTTS",
		"session_id": sessionID,
		"req_params": params,
	})
	return payload
}

func (p *TTSProvider) sendTTSEvent(ctx context.Context, conn *websocket.Conn, event int32, sessionID string, payload []byte, serialization byte) error {
	frame := buildTTSEventFrame(event, sessionID, payload, serialization)
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
	} else {
		_ = conn.SetWriteDeadline(time.Now().Add(p.cfg.Timeout))
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return fmt.Errorf("write volcengine TTS event %d: %w", event, err)
	}
	return nil
}

func (p *TTSProvider) expectTTSEvent(ctx context.Context, conn *websocket.Conn, event int32, logger *slog.Logger) error {
	resp, err := p.readTTSResponse(ctx, conn)
	if err != nil {
		return err
	}
	logTTSResponse(logger, resp, 0)
	if resp.messageType == messageTypeError {
		return fmt.Errorf("volcengine TTS error %d: %s", resp.errorCode, string(resp.payload))
	}
	if resp.event != event {
		return fmt.Errorf("volcengine TTS expected event %d, got %d: %s %s", event, resp.event, resp.meta, string(resp.payload))
	}
	return nil
}

func (p *TTSProvider) readTTSResponse(ctx context.Context, conn *websocket.Conn) (ttsResponse, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	} else {
		_ = conn.SetReadDeadline(time.Now().Add(p.cfg.Timeout))
	}
	mt, data, err := conn.ReadMessage()
	if err != nil {
		return ttsResponse{}, fmt.Errorf("read volcengine TTS response: %w", err)
	}
	if mt != websocket.BinaryMessage {
		return ttsResponse{}, nil
	}
	resp, err := parseTTSResponse(data)
	if err != nil {
		return ttsResponse{}, err
	}
	return resp, nil
}

func logTTSEventSent(logger *slog.Logger, event int32, sessionID string, payload []byte, serialization byte) {
	attrs := []any{
		"event", ttsEventName(event),
		"event_id", event,
		"payload_bytes", len(payload),
		"serialization", serialization,
	}
	if sessionID != "" {
		attrs = append(attrs, "session_id", sessionID)
	}
	logger.Info("volcengine tts event sent", attrs...)
}

func logTTSResponse(logger *slog.Logger, resp ttsResponse, frame int64) {
	attrs := []any{
		"message_type", ttsMessageTypeName(resp.messageType),
		"message_type_id", resp.messageType,
		"event", ttsEventName(resp.event),
		"event_id", resp.event,
		"payload_bytes", len(resp.payload),
	}
	if frame > 0 {
		attrs = append(attrs, "frame", frame)
	}
	if resp.sessionID != "" {
		attrs = append(attrs, "session_id", resp.sessionID)
	}
	if resp.connection != "" {
		attrs = append(attrs, "connection", resp.connection)
	}
	if resp.meta != "" {
		attrs = append(attrs, "meta", textPreview(resp.meta, 200))
	}
	if resp.messageType == messageTypeError && len(resp.payload) > 0 {
		attrs = append(attrs, "payload", textPreview(string(resp.payload), 200))
	}
	if resp.messageType == messageTypeAudioResult {
		attrs = append(attrs, "audio_bytes", len(resp.payload))
	}
	logger.Info("volcengine tts response event", attrs...)
}

func ttsEventName(event int32) string {
	switch event {
	case ttsEventStartConnection:
		return "StartConnection"
	case ttsEventFinishConnection:
		return "FinishConnection"
	case ttsEventConnectionStart:
		return "ConnectionStart"
	case ttsEventConnectionFail:
		return "ConnectionFail"
	case ttsEventStartSession:
		return "StartSession"
	case ttsEventFinishSession:
		return "FinishSession"
	case ttsEventSessionStart:
		return "SessionStart"
	case ttsEventSessionFinish:
		return "SessionFinish"
	case ttsEventSessionFail:
		return "SessionFail"
	case ttsEventTaskRequest:
		return "TaskRequest"
	case ttsEventSentenceStart:
		return "SentenceStart"
	case ttsEventSentenceEnd:
		return "SentenceEnd"
	case ttsEventResponse:
		return "Response"
	default:
		if event == 0 {
			return ""
		}
		return "Unknown"
	}
}

func ttsMessageTypeName(messageType byte) string {
	switch messageType {
	case messageTypeFullServerResult:
		return "FullServerResult"
	case messageTypeAudioResult:
		return "AudioResult"
	case messageTypeError:
		return "Error"
	default:
		if messageType == 0 {
			return ""
		}
		return "Unknown"
	}
}

type ttsResponse struct {
	messageType byte
	flags       byte
	event       int32
	sessionID   string
	connection  string
	meta        string
	errorCode   int32
	payload     []byte
}

func buildTTSEventFrame(event int32, sessionID string, payload []byte, serialization byte) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 16+len(sessionID)+len(payload)))
	buf.WriteByte(protocolVersion<<4 | headerWords)
	buf.WriteByte(messageTypeFullClientRequest<<4 | ttsFlagWithEvent)
	buf.WriteByte(serialization<<4 | compressionNone)
	buf.WriteByte(0)
	_ = binary.Write(buf, binary.BigEndian, event)
	if sessionID != "" {
		writeSizedString(buf, sessionID)
	}
	if payload != nil {
		_ = binary.Write(buf, binary.BigEndian, int32(len(payload)))
		buf.Write(payload)
	}
	return buf.Bytes()
}

func parseTTSResponse(data []byte) (ttsResponse, error) {
	if len(data) < 4 {
		return ttsResponse{}, fmt.Errorf("volcengine TTS response too short: %d bytes", len(data))
	}
	resp := ttsResponse{
		messageType: data[1] >> 4,
		flags:       data[1] & 0x0f,
	}
	offset := int(data[0]&0x0f) * 4
	if offset < 4 || offset > len(data) {
		return ttsResponse{}, fmt.Errorf("invalid volcengine TTS header size %d", offset)
	}
	switch resp.messageType {
	case messageTypeFullServerResult, messageTypeAudioResult:
		if resp.flags&ttsFlagWithEvent != 0 {
			event, next, err := readInt32(data, offset)
			if err != nil {
				return ttsResponse{}, err
			}
			resp.event = event
			offset = next
			switch resp.event {
			case ttsEventConnectionStart:
				resp.connection, offset, err = readSizedString(data, offset)
			case ttsEventConnectionFail:
				resp.meta, offset, err = readSizedString(data, offset)
			case ttsEventSessionStart, ttsEventSessionFail, ttsEventSessionFinish:
				resp.sessionID, offset, err = readSizedString(data, offset)
				if err == nil {
					resp.meta, offset, err = readSizedString(data, offset)
				}
			case ttsEventResponse, ttsEventSentenceStart, ttsEventSentenceEnd:
				resp.sessionID, offset, err = readSizedString(data, offset)
			}
			if err != nil {
				return ttsResponse{}, err
			}
		}
		if offset+4 <= len(data) {
			payload, _, err := readSizedBytes(data, offset)
			if err != nil {
				return ttsResponse{}, err
			}
			resp.payload = payload
		}
	case messageTypeError:
		code, next, err := readInt32(data, offset)
		if err != nil {
			return ttsResponse{}, err
		}
		resp.errorCode = code
		if next+4 <= len(data) {
			resp.payload, _, err = readSizedBytes(data, next)
			if err != nil {
				return ttsResponse{}, err
			}
		}
	default:
		return ttsResponse{}, fmt.Errorf("unexpected volcengine TTS message type %d", resp.messageType)
	}
	return resp, nil
}

func writeSizedString(w io.Writer, value string) {
	encoded := []byte(value)
	_ = binary.Write(w, binary.BigEndian, int32(len(encoded)))
	_, _ = w.Write(encoded)
}

func readInt32(data []byte, offset int) (int32, int, error) {
	if offset+4 > len(data) {
		return 0, offset, errors.New("volcengine TTS response truncated int32")
	}
	return int32(binary.BigEndian.Uint32(data[offset : offset+4])), offset + 4, nil
}

func readSizedString(data []byte, offset int) (string, int, error) {
	payload, next, err := readSizedBytes(data, offset)
	if err != nil {
		return "", offset, err
	}
	return string(payload), next, nil
}

func readSizedBytes(data []byte, offset int) ([]byte, int, error) {
	size, next, err := readInt32(data, offset)
	if err != nil {
		return nil, offset, err
	}
	if size < 0 || next+int(size) > len(data) {
		return nil, offset, fmt.Errorf("invalid volcengine TTS payload size %d", size)
	}
	return data[next : next+int(size)], next + int(size), nil
}
