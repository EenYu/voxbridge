package media

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"voxbridge/internal/playback"
)

type receivedWSMessage struct {
	messageType int
	payload     []byte
}

func TestWebSocketAudioSenderStreamsRawBinaryPCMInWSBinaryMode(t *testing.T) {
	conn, messages, cleanup := newTestWebSocketConn(t, 2)
	defer cleanup()
	var logs bytes.Buffer

	sender := newWebSocketAudioSender(conn, 16000, 4, playback.ModeWSBinary, testLogger(&logs))
	if err := sender.SendPCM(context.Background(), []byte{0, 1, 2, 3, 4, 5}); err != nil {
		t.Fatalf("SendPCM() error = %v", err)
	}

	first := mustReceiveWSMessage(t, messages)
	if first.messageType != websocket.BinaryMessage || string(first.payload) != string([]byte{0, 1, 2, 3}) {
		t.Fatalf("first message = %#v", first)
	}

	second := mustReceiveWSMessage(t, messages)
	if second.messageType != websocket.BinaryMessage || string(second.payload) != string([]byte{4, 5}) {
		t.Fatalf("second message = %#v", second)
	}

	assertFirstWebSocketWriteLog(t, logs.String(), map[string]string{
		"playback_mode": "ws_binary",
		"message_type":  "binary",
		"chunk_bytes":   "4",
		"chunk_ms":      "0.125",
	})
}

func TestWebSocketAudioSenderUsesStreamAudioJSONInUUIDBroadcastMode(t *testing.T) {
	conn, messages, cleanup := newTestWebSocketConn(t, 1)
	defer cleanup()
	var logs bytes.Buffer

	sender := newWebSocketAudioSender(conn, 16000, 4, playback.ModeUUIDBroadcast, testLogger(&logs))
	if err := sender.SendPCM(context.Background(), []byte{0, 1, 2, 3}); err != nil {
		t.Fatalf("SendPCM() error = %v", err)
	}

	msg := mustReceiveWSMessage(t, messages)
	if msg.messageType != websocket.TextMessage {
		t.Fatalf("message type = %d, want text", msg.messageType)
	}

	var got struct {
		Type string `json:"type"`
		Data struct {
			SampleRate int `json:"sampleRate"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg.payload, &got); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}
	if got.Type != "streamAudio" || got.Data.SampleRate != 16000 {
		t.Fatalf("message = %+v", got)
	}

	assertFirstWebSocketWriteLog(t, logs.String(), map[string]string{
		"playback_mode": "uuid_broadcast",
		"message_type":  "json",
		"chunk_bytes":   "4",
		"chunk_ms":      "0.125",
	})
}

func TestWebSocketAudioSenderClearQueuedSendsClearAudioControl(t *testing.T) {
	conn, messages, cleanup := newTestWebSocketConn(t, 1)
	defer cleanup()
	var logs bytes.Buffer

	sender := newWebSocketAudioSender(conn, 16000, 4, playback.ModeWSBinary, testLogger(&logs))
	sender.ClearQueued()

	msg := mustReceiveWSMessage(t, messages)
	if msg.messageType != websocket.TextMessage {
		t.Fatalf("message type = %d, want text", msg.messageType)
	}

	var got clearAudioMessage
	if err := json.Unmarshal(msg.payload, &got); err != nil {
		t.Fatalf("unmarshal clearAudio: %v", err)
	}
	if got.Type != "clearAudio" {
		t.Fatalf("clearAudio type = %q", got.Type)
	}
	if strings.Contains(logs.String(), "first_websocket_write") {
		t.Fatalf("ClearQueued logged first_websocket_write; logs:\n%s", logs.String())
	}
}

func TestWebSocketAudioSenderReserveWriteDelayPacesAndReset(t *testing.T) {
	sender := newWebSocketAudioSender(nil, 10, 2, playback.ModeWSBinary, nil)
	base := time.Unix(0, 0)

	if delay := sender.reserveWriteDelay(base, 2); delay != 0 {
		t.Fatalf("first delay = %v, want 0", delay)
	}

	if delay := sender.reserveWriteDelay(base.Add(30*time.Millisecond), 2); delay != 70*time.Millisecond {
		t.Fatalf("second delay = %v, want 70ms", delay)
	}

	sender.resetPacing()
	if delay := sender.reserveWriteDelay(base.Add(30*time.Millisecond), 2); delay != 0 {
		t.Fatalf("delay after reset = %v, want 0", delay)
	}
}

func newTestWebSocketConn(t *testing.T, expectedMessages int) (*websocket.Conn, <-chan receivedWSMessage, func()) {
	t.Helper()

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	messages := make(chan receivedWSMessage, expectedMessages)
	errCh := make(chan error, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()

		for i := 0; i < expectedMessages; i++ {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			messages <- receivedWSMessage{messageType: messageType, payload: append([]byte(nil), payload...)}
		}
		errCh <- nil
	}))

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		server.Close()
		t.Fatalf("dial websocket: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Close()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("websocket server read failed: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for websocket server to finish")
		}
	}

	return conn, messages, cleanup
}

func mustReceiveWSMessage(t *testing.T, messages <-chan receivedWSMessage) receivedWSMessage {
	t.Helper()

	select {
	case msg := <-messages:
		return msg
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for websocket message")
		return receivedWSMessage{}
	}
}

func testLogger(out *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(out, nil))
}

func assertFirstWebSocketWriteLog(t *testing.T, logs string, fields map[string]string) {
	t.Helper()

	if count := strings.Count(logs, "msg=first_websocket_write"); count != 1 {
		t.Fatalf("first_websocket_write log count = %d, want 1; logs:\n%s", count, logs)
	}
	if !strings.Contains(logs, "write_duration_ms=") {
		t.Fatalf("logs missing write_duration_ms field:\n%s", logs)
	}
	for key, value := range fields {
		want := key + "=" + value
		if !strings.Contains(logs, want) {
			t.Fatalf("logs missing %q:\n%s", want, logs)
		}
	}
}
