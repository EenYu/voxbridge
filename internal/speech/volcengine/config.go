package volcengine

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultSTTEndpoint = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel"
	defaultTTSEndpoint = "wss://openspeech.bytedance.com/api/v3/tts/bidirection"

	defaultAudioFormat = "pcm"
	defaultSampleRate  = 16000
	defaultBits        = 16
	defaultChannels    = 1

	defaultAsyncEndWindowSize     = 600
	defaultAsyncForceToSpeechTime = 800
)

type wsDialer interface {
	DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (*websocket.Conn, *http.Response, error)
}

type AudioConfig struct {
	Format     string `json:"format"`
	Codec      string `json:"codec,omitempty"`
	Rate       int    `json:"rate"`
	Bits       int    `json:"bits,omitempty"`
	Channel    int    `json:"channel"`
	Language   string `json:"language,omitempty"`
	VoiceType  string `json:"voice_type,omitempty"`
	Encoding   string `json:"encoding,omitempty"`
	SpeechRate int    `json:"speech_rate,omitempty"`
	PitchRate  int    `json:"pitch_rate,omitempty"`
}

func defaultAudioConfig() AudioConfig {
	return AudioConfig{
		Format:  defaultAudioFormat,
		Rate:    defaultSampleRate,
		Bits:    defaultBits,
		Channel: defaultChannels,
	}
}

type STTConfig struct {
	Endpoint   string
	AppID      string
	AccessKey  string
	Token      string
	ResourceID string
	Cluster    string
	Workflow   string
	Audio      AudioConfig
	Timeout    time.Duration
	ChunkSize  int
	Logger     *slog.Logger

	AsyncEndWindowSize     int
	AsyncForceToSpeechTime int

	dialer wsDialer
}

func (c STTConfig) withDefaults() STTConfig {
	if c.Endpoint == "" {
		c.Endpoint = defaultSTTEndpoint
	}
	if c.Audio.Format == "" {
		c.Audio.Format = defaultAudioFormat
	}
	if c.Audio.Rate == 0 {
		c.Audio.Rate = defaultSampleRate
	}
	if c.Audio.Bits == 0 {
		c.Audio.Bits = defaultBits
	}
	if c.Audio.Channel == 0 {
		c.Audio.Channel = defaultChannels
	}
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
	if c.ChunkSize <= 0 {
		c.ChunkSize = 3200
	}
	if c.AsyncEndWindowSize == 0 {
		c.AsyncEndWindowSize = defaultAsyncEndWindowSize
	}
	if c.AsyncForceToSpeechTime == 0 {
		c.AsyncForceToSpeechTime = defaultAsyncForceToSpeechTime
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.dialer == nil {
		c.dialer = websocket.DefaultDialer
	}
	return c
}

func (c STTConfig) validate() error {
	if c.AppID == "" {
		return errors.New("volcengine STT AppID is required")
	}
	if c.Token == "" && c.AccessKey == "" {
		return errors.New("volcengine STT Token or AccessKey is required")
	}
	if c.ResourceID == "" && c.Cluster == "" {
		return errors.New("volcengine STT ResourceID or Cluster is required")
	}
	if c.AsyncEndWindowSize <= 0 {
		return errors.New("volcengine STT async end window size must be positive")
	}
	if c.AsyncForceToSpeechTime <= 0 {
		return errors.New("volcengine STT async force to speech time must be positive")
	}
	return nil
}

func (c STTConfig) isAsync() bool {
	return strings.Contains(c.Endpoint, "bigmodel_async")
}

type TTSConfig struct {
	Endpoint   string
	AppID      string
	AccessKey  string
	Token      string
	ResourceID string
	Cluster    string
	VoiceType  string
	Audio      AudioConfig
	Timeout    time.Duration
	Logger     *slog.Logger

	dialer wsDialer
}

func (c TTSConfig) withDefaults() TTSConfig {
	if c.Endpoint == "" {
		c.Endpoint = defaultTTSEndpoint
	}
	if c.Audio.Format == "" {
		c.Audio.Format = defaultAudioFormat
	}
	if c.Audio.Rate == 0 {
		c.Audio.Rate = defaultSampleRate
	}
	if c.Audio.Bits == 0 {
		c.Audio.Bits = defaultBits
	}
	if c.Audio.Channel == 0 {
		c.Audio.Channel = defaultChannels
	}
	if c.Audio.VoiceType == "" {
		c.Audio.VoiceType = c.VoiceType
	}
	if c.Audio.VoiceType == "" {
		c.Audio.VoiceType = "zh_female_wanwanxiaohe_moon_bigtts"
	}
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.dialer == nil {
		c.dialer = websocket.DefaultDialer
	}
	return c
}

func (c TTSConfig) validate() error {
	if c.AppID == "" {
		return errors.New("volcengine TTS AppID is required")
	}
	if c.Token == "" && c.AccessKey == "" {
		return errors.New("volcengine TTS Token or AccessKey is required")
	}
	if c.ResourceID == "" && c.Cluster == "" {
		return errors.New("volcengine TTS ResourceID or Cluster is required")
	}
	if c.Audio.VoiceType == "" {
		return errors.New("volcengine TTS voice type is required")
	}
	return nil
}

func authHeaders(appID, token, accessKey, resourceID, sessionID string) http.Header {
	h := http.Header{}
	if appID != "" {
		h.Set("X-Api-App-Key", appID)
		h.Set("X-Api-App-ID", appID)
	}
	if token != "" {
		h.Set("X-Api-Access-Key", token)
		h.Set("Authorization", "Bearer "+token)
	} else if accessKey != "" {
		h.Set("X-Api-Access-Key", accessKey)
		h.Set("Authorization", "Bearer "+accessKey)
	}
	if resourceID != "" {
		h.Set("X-Api-Resource-Id", resourceID)
	}
	if sessionID != "" {
		h.Set("X-Api-Connect-Id", sessionID)
		h.Set("X-Api-Request-Id", sessionID)
	}
	return h
}
