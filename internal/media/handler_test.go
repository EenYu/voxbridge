package media

import (
	"context"
	"encoding/json"
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

	sender := newWebSocketAudioSender(conn, 16000, 4, playback.ModeWSBinary)
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
}

func TestWebSocketAudioSenderUsesStreamAudioJSONInUUIDBroadcastMode(t *testing.T) {
	conn, messages, cleanup := newTestWebSocketConn(t, 1)
	defer cleanup()

	sender := newWebSocketAudioSender(conn, 16000, 4, playback.ModeUUIDBroadcast)
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
}

func TestWebSocketAudioSenderClearQueuedSendsClearAudioControl(t *testing.T) {
	conn, messages, cleanup := newTestWebSocketConn(t, 1)
	defer cleanup()

	sender := newWebSocketAudioSender(conn, 16000, 4, playback.ModeWSBinary)
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
}

func TestWebSocketAudioSenderReserveWriteDelayPacesAndReset(t *testing.T) {
	sender := newWebSocketAudioSender(nil, 10, 2, playback.ModeWSBinary)
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
