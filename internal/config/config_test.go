package config

import (
	"strings"
	"testing"
	"time"

	"voxbridge/internal/playback"
)

func TestLoadFromEnvValidatesAndAppliesDefaults(t *testing.T) {
	env := map[string]string{
		"VOXBRIDGE_ESL_ADDRESS":                               "127.0.0.1:8021",
		"VOXBRIDGE_ESL_PASSWORD":                              "ClueCon",
		"VOXBRIDGE_PUBLIC_WS_URL":                             "wss://voice.example.com/audio",
		"VOXBRIDGE_LLM_BASE_URL":                              "https://llm.example.com",
		"VOXBRIDGE_LLM_API_KEY":                               "test-key",
		"VOXBRIDGE_LLM_MODEL":                                 "gpt-test",
		"VOXBRIDGE_VOLCENGINE_ACCESS_KEY_ID":                  "ak",
		"VOXBRIDGE_VOLCENGINE_SECRET_ACCESS_KEY":              "sk",
		"VOXBRIDGE_VOLCENGINE_APP_ID":                         "app",
		"VOXBRIDGE_VOLCENGINE_CLUSTER":                        "volcano_icl",
		"VOXBRIDGE_VOLCENGINE_REGION":                         "cn-north-1",
		"VOXBRIDGE_LLM_TIMEOUT":                               "10s",
		"VOXBRIDGE_ESL_DIAL_TIMEOUT":                          "3s",
		"VOXBRIDGE_SPEECH_TIMEOUT":                            "15s",
		"VOXBRIDGE_AUDIO_SAMPLE_RATE_HZ":                      "8000",
		"VOXBRIDGE_AUDIO_CHANNELS":                            "1",
		"VOXBRIDGE_AUDIO_FRAME_MS":                            "40",
		"VOXBRIDGE_AUDIO_CODEC":                               "pcm_s16le",
		"VOXBRIDGE_VOLCENGINE_STT_ASYNC_END_WINDOW_SIZE":      "700",
		"VOXBRIDGE_VOLCENGINE_STT_ASYNC_FORCE_TO_SPEECH_TIME": "900",
	}

	cfg, err := LoadFromEnv(mapLookup(env))
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.ESL.Address != "127.0.0.1:8021" {
		t.Fatalf("ESL address = %q", cfg.ESL.Address)
	}
	if cfg.LLM.Timeout != 10*time.Second {
		t.Fatalf("LLM timeout = %s", cfg.LLM.Timeout)
	}
	if cfg.Timeouts.ESLDial != 3*time.Second {
		t.Fatalf("ESL dial timeout = %s", cfg.Timeouts.ESLDial)
	}
	if cfg.Audio.SampleRateHz != 8000 || cfg.Audio.FrameMS != 40 {
		t.Fatalf("audio config = %+v", cfg.Audio)
	}
	if cfg.Volcengine.STTAsyncEndWindowSize != 700 {
		t.Fatalf("STT async end window size = %d", cfg.Volcengine.STTAsyncEndWindowSize)
	}
	if cfg.Volcengine.STTAsyncForceToSpeechTime != 900 {
		t.Fatalf("STT async force to speech time = %d", cfg.Volcengine.STTAsyncForceToSpeechTime)
	}
	if cfg.PlaybackMode != playback.ModeWSBinary {
		t.Fatalf("playback mode = %q, want %q", cfg.PlaybackMode, playback.ModeWSBinary)
	}
}

func TestLoadFromEnvDefaultsOptionalAudioAndTimeouts(t *testing.T) {
	env := map[string]string{
		"VOXBRIDGE_ESL_ADDRESS":                  "127.0.0.1:8021",
		"VOXBRIDGE_ESL_PASSWORD":                 "ClueCon",
		"VOXBRIDGE_PUBLIC_WS_URL":                "ws://localhost:8080/audio",
		"VOXBRIDGE_LLM_BASE_URL":                 "http://localhost:11434",
		"VOXBRIDGE_LLM_API_KEY":                  "test-key",
		"VOXBRIDGE_LLM_MODEL":                    "gpt-test",
		"VOXBRIDGE_VOLCENGINE_ACCESS_KEY_ID":     "ak",
		"VOXBRIDGE_VOLCENGINE_SECRET_ACCESS_KEY": "sk",
		"VOXBRIDGE_VOLCENGINE_APP_ID":            "app",
		"VOXBRIDGE_VOLCENGINE_RESOURCE_ID":       "resource",
	}

	cfg, err := LoadFromEnv(mapLookup(env))
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.Audio.SampleRateHz != defaultAudioSampleRateHz {
		t.Fatalf("sample rate = %d", cfg.Audio.SampleRateHz)
	}
	if cfg.Audio.Channels != defaultAudioChannels {
		t.Fatalf("channels = %d", cfg.Audio.Channels)
	}
	if cfg.Audio.Codec != "pcm_s16le" {
		t.Fatalf("codec = %q", cfg.Audio.Codec)
	}
	if cfg.LLM.Timeout != defaultLLMTimeout {
		t.Fatalf("LLM timeout = %s", cfg.LLM.Timeout)
	}
	if cfg.Volcengine.STTAsyncEndWindowSize != defaultSTTAsyncEndWindowSize {
		t.Fatalf("STT async end window size = %d", cfg.Volcengine.STTAsyncEndWindowSize)
	}
	if cfg.Volcengine.STTAsyncForceToSpeechTime != defaultSTTAsyncForceToSpeechTime {
		t.Fatalf("STT async force to speech time = %d", cfg.Volcengine.STTAsyncForceToSpeechTime)
	}
	if cfg.PlaybackMode != defaultPlaybackMode {
		t.Fatalf("playback mode = %q, want %q", cfg.PlaybackMode, defaultPlaybackMode)
	}
}

func TestLoadFromEnvAcceptsUUIDBroadcastPlaybackMode(t *testing.T) {
	env := map[string]string{
		"VOXBRIDGE_ESL_ADDRESS":                  "127.0.0.1:8021",
		"VOXBRIDGE_ESL_PASSWORD":                 "ClueCon",
		"VOXBRIDGE_PUBLIC_WS_URL":                "ws://localhost:8080/audio",
		"VOXBRIDGE_PLAYBACK_MODE":                string(playback.ModeUUIDBroadcast),
		"VOXBRIDGE_LLM_BASE_URL":                 "http://localhost:11434",
		"VOXBRIDGE_LLM_API_KEY":                  "test-key",
		"VOXBRIDGE_LLM_MODEL":                    "gpt-test",
		"VOXBRIDGE_VOLCENGINE_ACCESS_KEY_ID":     "ak",
		"VOXBRIDGE_VOLCENGINE_SECRET_ACCESS_KEY": "sk",
		"VOXBRIDGE_VOLCENGINE_APP_ID":            "app",
		"VOXBRIDGE_VOLCENGINE_RESOURCE_ID":       "resource",
	}

	cfg, err := LoadFromEnv(mapLookup(env))
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if cfg.PlaybackMode != playback.ModeUUIDBroadcast {
		t.Fatalf("playback mode = %q, want %q", cfg.PlaybackMode, playback.ModeUUIDBroadcast)
	}
}

func TestLoadFromEnvReturnsValidationErrors(t *testing.T) {
	env := map[string]string{
		"VOXBRIDGE_PUBLIC_WS_URL":                             "https://example.com/audio",
		"VOXBRIDGE_PLAYBACK_MODE":                             "bad-mode",
		"VOXBRIDGE_LLM_BASE_URL":                              "ftp://example.com",
		"VOXBRIDGE_AUDIO_SAMPLE_RATE_HZ":                      "-1",
		"VOXBRIDGE_LLM_TIMEOUT":                               "bad-duration",
		"VOXBRIDGE_VOLCENGINE_STT_ASYNC_END_WINDOW_SIZE":      "0",
		"VOXBRIDGE_VOLCENGINE_STT_ASYNC_FORCE_TO_SPEECH_TIME": "-1",
	}

	_, err := LoadFromEnv(mapLookup(env))
	if err == nil {
		t.Fatal("LoadFromEnv() error = nil")
	}

	message := err.Error()
	for _, want := range []string{
		"VOXBRIDGE_ESL_ADDRESS is required",
		"VOXBRIDGE_ESL_PASSWORD is required",
		"VOXBRIDGE_LLM_MODEL is required",
		"VOXBRIDGE_PUBLIC_WS_URL must use ws or wss",
		"VOXBRIDGE_PLAYBACK_MODE must be one of",
		"VOXBRIDGE_LLM_BASE_URL must use http or https",
		"VOXBRIDGE_AUDIO_SAMPLE_RATE_HZ must be positive",
		"VOXBRIDGE_LLM_TIMEOUT must be a positive duration",
		"VOXBRIDGE_VOLCENGINE_STT_ASYNC_END_WINDOW_SIZE must be positive",
		"VOXBRIDGE_VOLCENGINE_STT_ASYNC_FORCE_TO_SPEECH_TIME must be positive",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("validation error %q missing from %q", want, message)
		}
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
