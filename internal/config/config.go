package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAudioSampleRateHz         = 16000
	defaultAudioChannels             = 1
	defaultAudioFrameMS              = 20
	defaultLLMTimeout                = 60 * time.Second
	defaultESLTimeout                = 5 * time.Second
	defaultSpeechTimeout             = 30 * time.Second
	defaultSTTAsyncEndWindowSize     = 600
	defaultSTTAsyncForceToSpeechTime = 800
)

type Config struct {
	HTTP       HTTPConfig
	ESL        ESLConfig
	PublicWS   PublicWSConfig
	LLM        LLMConfig
	Volcengine VolcengineConfig
	Audio      AudioConfig
	Timeouts   TimeoutConfig
}

type HTTPConfig struct {
	Address string
}

type ESLConfig struct {
	Address  string
	Password string
}

type PublicWSConfig struct {
	URL string
}

type LLMConfig struct {
	BaseURL string
	APIKey  string
	Model   string
	Timeout time.Duration
}

type VolcengineConfig struct {
	AccessKeyID               string
	SecretAccessKey           string
	AccessKey                 string
	Token                     string
	AppID                     string
	Cluster                   string
	Region                    string
	ResourceID                string
	STTEndpoint               string
	STTResourceID             string
	STTCluster                string
	TTSEndpoint               string
	TTSResourceID             string
	TTSCluster                string
	VoiceType                 string
	STTAsyncEndWindowSize     int
	STTAsyncForceToSpeechTime int
}

type AudioConfig struct {
	SampleRateHz int
	Channels     int
	FrameMS      int
	Codec        string
}

type TimeoutConfig struct {
	ESLDial time.Duration
	Speech  time.Duration
}

// Load reads process environment variables and validates the resulting config.
func Load() (Config, error) {
	return LoadFromEnv(os.LookupEnv)
}

// LoadFromEnv reads config through lookup, which keeps tests isolated from the
// real process environment.
func LoadFromEnv(lookup func(string) (string, bool)) (Config, error) {
	cfg := Config{
		HTTP: HTTPConfig{
			Address: valueOrDefault(get(lookup, "VOXBRIDGE_HTTP_ADDR"), ":8080"),
		},
		ESL: ESLConfig{
			Address:  get(lookup, "VOXBRIDGE_ESL_ADDRESS"),
			Password: get(lookup, "VOXBRIDGE_ESL_PASSWORD"),
		},
		PublicWS: PublicWSConfig{
			URL: get(lookup, "VOXBRIDGE_PUBLIC_WS_URL"),
		},
		LLM: LLMConfig{
			BaseURL: get(lookup, "VOXBRIDGE_LLM_BASE_URL"),
			APIKey:  get(lookup, "VOXBRIDGE_LLM_API_KEY"),
			Model:   get(lookup, "VOXBRIDGE_LLM_MODEL"),
			Timeout: duration(lookup, "VOXBRIDGE_LLM_TIMEOUT", defaultLLMTimeout),
		},
		Volcengine: VolcengineConfig{
			AccessKeyID:               get(lookup, "VOXBRIDGE_VOLCENGINE_ACCESS_KEY_ID"),
			SecretAccessKey:           get(lookup, "VOXBRIDGE_VOLCENGINE_SECRET_ACCESS_KEY"),
			AccessKey:                 get(lookup, "VOXBRIDGE_VOLCENGINE_ACCESS_KEY"),
			Token:                     get(lookup, "VOXBRIDGE_VOLCENGINE_TOKEN"),
			AppID:                     get(lookup, "VOXBRIDGE_VOLCENGINE_APP_ID"),
			Cluster:                   get(lookup, "VOXBRIDGE_VOLCENGINE_CLUSTER"),
			Region:                    get(lookup, "VOXBRIDGE_VOLCENGINE_REGION"),
			ResourceID:                get(lookup, "VOXBRIDGE_VOLCENGINE_RESOURCE_ID"),
			STTEndpoint:               get(lookup, "VOXBRIDGE_VOLCENGINE_STT_ENDPOINT"),
			STTResourceID:             get(lookup, "VOXBRIDGE_VOLCENGINE_STT_RESOURCE_ID"),
			STTCluster:                get(lookup, "VOXBRIDGE_VOLCENGINE_STT_CLUSTER"),
			TTSEndpoint:               get(lookup, "VOXBRIDGE_VOLCENGINE_TTS_ENDPOINT"),
			TTSResourceID:             get(lookup, "VOXBRIDGE_VOLCENGINE_TTS_RESOURCE_ID"),
			TTSCluster:                get(lookup, "VOXBRIDGE_VOLCENGINE_TTS_CLUSTER"),
			VoiceType:                 get(lookup, "VOXBRIDGE_VOLCENGINE_TTS_VOICE_TYPE"),
			STTAsyncEndWindowSize:     integer(lookup, "VOXBRIDGE_VOLCENGINE_STT_ASYNC_END_WINDOW_SIZE", defaultSTTAsyncEndWindowSize),
			STTAsyncForceToSpeechTime: integer(lookup, "VOXBRIDGE_VOLCENGINE_STT_ASYNC_FORCE_TO_SPEECH_TIME", defaultSTTAsyncForceToSpeechTime),
		},
		Audio: AudioConfig{
			SampleRateHz: integer(lookup, "VOXBRIDGE_AUDIO_SAMPLE_RATE_HZ", defaultAudioSampleRateHz),
			Channels:     integer(lookup, "VOXBRIDGE_AUDIO_CHANNELS", defaultAudioChannels),
			FrameMS:      integer(lookup, "VOXBRIDGE_AUDIO_FRAME_MS", defaultAudioFrameMS),
			Codec:        valueOrDefault(get(lookup, "VOXBRIDGE_AUDIO_CODEC"), "pcm_s16le"),
		},
		Timeouts: TimeoutConfig{
			ESLDial: duration(lookup, "VOXBRIDGE_ESL_DIAL_TIMEOUT", defaultESLTimeout),
			Speech:  duration(lookup, "VOXBRIDGE_SPEECH_TIMEOUT", defaultSpeechTimeout),
		},
	}

	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	var errs []error

	require(&errs, "VOXBRIDGE_ESL_ADDRESS", c.ESL.Address)
	require(&errs, "VOXBRIDGE_ESL_PASSWORD", c.ESL.Password)
	require(&errs, "VOXBRIDGE_PUBLIC_WS_URL", c.PublicWS.URL)
	require(&errs, "VOXBRIDGE_LLM_BASE_URL", c.LLM.BaseURL)
	require(&errs, "VOXBRIDGE_LLM_API_KEY", c.LLM.APIKey)
	require(&errs, "VOXBRIDGE_LLM_MODEL", c.LLM.Model)
	require(&errs, "VOXBRIDGE_VOLCENGINE_APP_ID", c.Volcengine.AppID)
	if strings.TrimSpace(c.Volcengine.Token) == "" &&
		strings.TrimSpace(c.Volcengine.AccessKey) == "" &&
		strings.TrimSpace(c.Volcengine.AccessKeyID) == "" &&
		strings.TrimSpace(c.Volcengine.SecretAccessKey) == "" {
		errs = append(errs, errors.New("one of VOXBRIDGE_VOLCENGINE_TOKEN, VOXBRIDGE_VOLCENGINE_ACCESS_KEY, VOXBRIDGE_VOLCENGINE_ACCESS_KEY_ID, or VOXBRIDGE_VOLCENGINE_SECRET_ACCESS_KEY is required"))
	}
	sttSpeechTarget := firstNonEmpty(c.Volcengine.ResourceID, c.Volcengine.Cluster, c.Volcengine.STTResourceID, c.Volcengine.STTCluster)
	ttsSpeechTarget := firstNonEmpty(c.Volcengine.ResourceID, c.Volcengine.Cluster, c.Volcengine.TTSResourceID, c.Volcengine.TTSCluster)
	if sttSpeechTarget == "" || ttsSpeechTarget == "" {
		errs = append(errs, errors.New("VOXBRIDGE_VOLCENGINE_RESOURCE_ID, VOXBRIDGE_VOLCENGINE_CLUSTER, or STT/TTS-specific resource or cluster values are required"))
	}

	validateHTTPURL(&errs, "VOXBRIDGE_LLM_BASE_URL", c.LLM.BaseURL)
	validateWebSocketURL(&errs, "VOXBRIDGE_PUBLIC_WS_URL", c.PublicWS.URL)
	validateOptionalWebSocketURL(&errs, "VOXBRIDGE_VOLCENGINE_STT_ENDPOINT", c.Volcengine.STTEndpoint)
	validateOptionalWebSocketURL(&errs, "VOXBRIDGE_VOLCENGINE_TTS_ENDPOINT", c.Volcengine.TTSEndpoint)
	positiveInt(&errs, "VOXBRIDGE_VOLCENGINE_STT_ASYNC_END_WINDOW_SIZE", c.Volcengine.STTAsyncEndWindowSize)
	positiveInt(&errs, "VOXBRIDGE_VOLCENGINE_STT_ASYNC_FORCE_TO_SPEECH_TIME", c.Volcengine.STTAsyncForceToSpeechTime)
	positiveDuration(&errs, "VOXBRIDGE_LLM_TIMEOUT", c.LLM.Timeout)
	positiveDuration(&errs, "VOXBRIDGE_ESL_DIAL_TIMEOUT", c.Timeouts.ESLDial)
	positiveDuration(&errs, "VOXBRIDGE_SPEECH_TIMEOUT", c.Timeouts.Speech)

	if c.Audio.SampleRateHz <= 0 {
		errs = append(errs, errors.New("VOXBRIDGE_AUDIO_SAMPLE_RATE_HZ must be positive"))
	}
	if c.Audio.Channels <= 0 {
		errs = append(errs, errors.New("VOXBRIDGE_AUDIO_CHANNELS must be positive"))
	}
	if c.Audio.FrameMS <= 0 {
		errs = append(errs, errors.New("VOXBRIDGE_AUDIO_FRAME_MS must be positive"))
	}
	if strings.TrimSpace(c.Audio.Codec) == "" {
		errs = append(errs, errors.New("VOXBRIDGE_AUDIO_CODEC is required"))
	}

	return errors.Join(errs...)
}

func get(lookup func(string) (string, bool), key string) string {
	value, ok := lookup(key)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func integer(lookup func(string) (string, bool), key string, fallback int) int {
	value := get(lookup, key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func duration(lookup func(string) (string, bool), key string, fallback time.Duration) time.Duration {
	value := get(lookup, key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return parsed
}

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func require(errs *[]error, key, value string) {
	if strings.TrimSpace(value) == "" {
		*errs = append(*errs, fmt.Errorf("%s is required", key))
	}
}

func validateHTTPURL(errs *[]error, key, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		*errs = append(*errs, fmt.Errorf("%s must be an absolute http(s) URL", key))
		return
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		*errs = append(*errs, fmt.Errorf("%s must use http or https", key))
	}
}

func validateWebSocketURL(errs *[]error, key, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		*errs = append(*errs, fmt.Errorf("%s must be an absolute ws(s) URL", key))
		return
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		*errs = append(*errs, fmt.Errorf("%s must use ws or wss", key))
	}
}

func validateOptionalWebSocketURL(errs *[]error, key, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	validateWebSocketURL(errs, key, value)
}

func positiveDuration(errs *[]error, key string, value time.Duration) {
	if value <= 0 {
		*errs = append(*errs, fmt.Errorf("%s must be a positive duration", key))
	}
}

func positiveInt(errs *[]error, key string, value int) {
	if value <= 0 {
		*errs = append(*errs, fmt.Errorf("%s must be positive", key))
	}
}
