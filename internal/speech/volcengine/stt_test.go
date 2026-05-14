package volcengine

import (
	"testing"

	"voxbridge/internal/speech"
)

func TestParseRecognitionEventsRootText(t *testing.T) {
	events, err := parseRecognitionEvents([]byte(`{"text":"hello","is_final":false}`), false)
	if err != nil {
		t.Fatalf("parseRecognitionEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Kind != speech.RecognitionPartial || events[0].Text != "hello" {
		t.Fatalf("event = %#v, want partial hello", events[0])
	}
}

func TestParseRecognitionEventsNestedUtterances(t *testing.T) {
	payload := []byte(`{"result":{"utterances":[{"text":"first","definite":true},{"text":"second","definite":false}]}}`)
	events, err := parseRecognitionEvents(payload, false)
	if err != nil {
		t.Fatalf("parseRecognitionEvents() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(events))
	}
	if events[0].Kind != speech.RecognitionFinal || events[0].Text != "first" {
		t.Fatalf("event[0] = %#v, want final first", events[0])
	}
	if events[1].Kind != speech.RecognitionPartial || events[1].Text != "second" {
		t.Fatalf("event[1] = %#v, want partial second", events[1])
	}
}

func TestParseRecognitionEventsFrameFinal(t *testing.T) {
	events, err := parseRecognitionEvents([]byte(`{"result":{"text":"done"}}`), true)
	if err != nil {
		t.Fatalf("parseRecognitionEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Kind != speech.RecognitionFinal {
		t.Fatalf("event kind = %v, want final", events[0].Kind)
	}
}

func TestNewProvidersValidateCredentials(t *testing.T) {
	if _, err := NewSTTProvider(STTConfig{}); err == nil {
		t.Fatal("NewSTTProvider() error = nil, want missing credentials error")
	}
	if _, err := NewTTSProvider(TTSConfig{}); err == nil {
		t.Fatal("NewTTSProvider() error = nil, want missing credentials error")
	}
}

func TestSTTAsyncDefaults(t *testing.T) {
	provider, err := NewSTTProvider(STTConfig{
		AppID:      "app",
		Token:      "token",
		ResourceID: "resource",
		Endpoint:   "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async",
	})
	if err != nil {
		t.Fatalf("NewSTTProvider() error = %v", err)
	}

	session := sttSession{cfg: provider.cfg}
	request := session.asyncInitialRequest("session-1")
	requestConfig := request["request"].(map[string]any)

	if requestConfig["end_window_size"] != defaultAsyncEndWindowSize {
		t.Fatalf("end_window_size = %v, want %d", requestConfig["end_window_size"], defaultAsyncEndWindowSize)
	}
	if requestConfig["force_to_speech_time"] != defaultAsyncForceToSpeechTime {
		t.Fatalf("force_to_speech_time = %v, want %d", requestConfig["force_to_speech_time"], defaultAsyncForceToSpeechTime)
	}
}

func TestSTTAsyncUsesConfiguredSentenceBoundary(t *testing.T) {
	provider, err := NewSTTProvider(STTConfig{
		AppID:                  "app",
		Token:                  "token",
		ResourceID:             "resource",
		Endpoint:               "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async",
		AsyncEndWindowSize:     750,
		AsyncForceToSpeechTime: 950,
	})
	if err != nil {
		t.Fatalf("NewSTTProvider() error = %v", err)
	}

	session := sttSession{cfg: provider.cfg}
	request := session.asyncInitialRequest("session-1")
	requestConfig := request["request"].(map[string]any)

	if requestConfig["end_window_size"] != 750 {
		t.Fatalf("end_window_size = %v, want 750", requestConfig["end_window_size"])
	}
	if requestConfig["force_to_speech_time"] != 950 {
		t.Fatalf("force_to_speech_time = %v, want 950", requestConfig["force_to_speech_time"])
	}
}

func TestSTTAsyncValidatesSentenceBoundary(t *testing.T) {
	_, err := NewSTTProvider(STTConfig{
		AppID:              "app",
		Token:              "token",
		ResourceID:         "resource",
		AsyncEndWindowSize: -1,
	})
	if err == nil {
		t.Fatal("NewSTTProvider() error = nil, want invalid async end window size")
	}
}
