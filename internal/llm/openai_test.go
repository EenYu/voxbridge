package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseChatCompletionSSE(t *testing.T) {
	stream := strings.NewReader(strings.Join([]string{
		": keepalive",
		"",
		`data: {"choices":[{"delta":{"content":"hel"}}]}`,
		"",
		`data: {"choices":[{"delta":{"content":"lo"}}]}`,
		"",
		"data: [DONE]",
		"",
	}, "\n"))

	var got []Delta
	err := parseChatCompletionSSE(stream, func(delta Delta) error {
		got = append(got, delta)
		return nil
	})
	if err != nil {
		t.Fatalf("parseChatCompletionSSE() error = %v", err)
	}

	want := []Delta{{Content: "hel"}, {Content: "lo"}, {Done: true}}
	if len(got) != len(want) {
		t.Fatalf("got %d deltas, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delta[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestParseChatCompletionSSEFinishReasonEmitsDone(t *testing.T) {
	stream := strings.NewReader("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")

	var got []Delta
	err := parseChatCompletionSSE(stream, func(delta Delta) error {
		got = append(got, delta)
		return nil
	})
	if err != nil {
		t.Fatalf("parseChatCompletionSSE() error = %v", err)
	}
	if len(got) != 1 || !got[0].Done {
		t.Fatalf("deltas = %+v, want single done delta", got)
	}
}

func TestParseChatCompletionSSEInvalidJSON(t *testing.T) {
	err := parseChatCompletionSSE(strings.NewReader("data: nope\n\n"), func(Delta) error {
		return nil
	})
	if err == nil {
		t.Fatal("parseChatCompletionSSE() error = nil")
	}
}

func TestOpenAIClientStreamChatPostsStreamingRequest(t *testing.T) {
	var seenAuth string
	var seenAccept string
	var requestBody struct {
		Model    string    `json:"model"`
		Messages []Message `json:"messages"`
		Stream   bool      `json:"stream"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q", r.Method)
		}
		seenAuth = r.Header.Get("Authorization")
		seenAccept = r.Header.Get("Accept")
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	client, err := NewOpenAICompatibleClient(OpenAIOptions{
		BaseURL:    server.URL,
		APIKey:     "secret",
		Model:      "gpt-test",
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatalf("NewOpenAICompatibleClient() error = %v", err)
	}

	deltas, errs := client.StreamChat(context.Background(), []Message{{Role: RoleUser, Content: "hello"}})
	var got []Delta
	for delta := range deltas {
		got = append(got, delta)
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("StreamChat() error = %v", err)
		}
	}

	if seenAuth != "Bearer secret" {
		t.Fatalf("Authorization = %q", seenAuth)
	}
	if seenAccept != "text/event-stream" {
		t.Fatalf("Accept = %q", seenAccept)
	}
	if requestBody.Model != "gpt-test" || !requestBody.Stream {
		t.Fatalf("request body = %+v", requestBody)
	}
	if len(requestBody.Messages) != 1 || requestBody.Messages[0].Content != "hello" {
		t.Fatalf("messages = %+v", requestBody.Messages)
	}
	if len(got) != 2 || got[0].Content != "hi" || !got[1].Done {
		t.Fatalf("deltas = %+v", got)
	}
}

func TestOpenAIClientStreamChatReportsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad api key", http.StatusUnauthorized)
	}))
	defer server.Close()

	client, err := NewOpenAICompatibleClient(OpenAIOptions{
		BaseURL:    server.URL,
		Model:      "gpt-test",
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatalf("NewOpenAICompatibleClient() error = %v", err)
	}

	deltas, errs := client.StreamChat(context.Background(), []Message{{Role: RoleUser, Content: "hello"}})
	for range deltas {
		t.Fatal("unexpected delta")
	}

	var gotErr error
	for err := range errs {
		gotErr = err
	}
	if gotErr == nil || !strings.Contains(gotErr.Error(), "status 401") {
		t.Fatalf("error = %v, want status 401", gotErr)
	}
}
