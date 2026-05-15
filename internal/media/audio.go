package media

import (
	"encoding/base64"
	"time"
)

const (
	DefaultSampleRate          = 16000
	DefaultChannels            = 1
	DefaultBytesPerSample      = 2
	DefaultChunkDuration       = 150 * time.Millisecond
	MinChunkDuration           = 100 * time.Millisecond
	MaxChunkDuration           = 200 * time.Millisecond
	DefaultBinaryChunkDuration = 20 * time.Millisecond
	MinBinaryChunkDuration     = 20 * time.Millisecond
	MaxBinaryChunkDuration     = 100 * time.Millisecond
)

// ChunkSizeBytes returns a PCM chunk size aligned to a complete audio frame.
// The requested duration is clamped to the 100-200ms range expected by the
// FreeSWITCH media websocket path.
func ChunkSizeBytes(sampleRate, channels, bytesPerSample int, target time.Duration) int {
	return boundedChunkSizeBytes(sampleRate, channels, bytesPerSample, target, DefaultChunkDuration, MinChunkDuration, MaxChunkDuration)
}

// BinaryChunkSizeBytes returns a raw PCM chunk size for ws_binary playback.
// The requested duration is clamped to the 20-100ms range expected by
// mod_audio_duplex.
func BinaryChunkSizeBytes(sampleRate, channels, bytesPerSample int, target time.Duration) int {
	return boundedChunkSizeBytes(sampleRate, channels, bytesPerSample, target, DefaultBinaryChunkDuration, MinBinaryChunkDuration, MaxBinaryChunkDuration)
}

func boundedChunkSizeBytes(sampleRate, channels, bytesPerSample int, target, fallback, min, max time.Duration) int {
	if sampleRate <= 0 {
		sampleRate = DefaultSampleRate
	}
	if channels <= 0 {
		channels = DefaultChannels
	}
	if bytesPerSample <= 0 {
		bytesPerSample = DefaultBytesPerSample
	}
	if target <= 0 {
		target = fallback
	}
	if target < min {
		target = min
	}
	if target > max {
		target = max
	}

	frameSize := channels * bytesPerSample
	bytesPerSecond := sampleRate * frameSize
	size := int((time.Duration(bytesPerSecond) * target) / time.Second)
	if size < frameSize {
		return frameSize
	}
	return size - (size % frameSize)
}

type streamAudioMessage struct {
	Type string          `json:"type"`
	Data streamAudioData `json:"data"`
}

type streamAudioData struct {
	AudioDataType string `json:"audioDataType"`
	SampleRate    int    `json:"sampleRate"`
	AudioData     string `json:"audioData"`
}

func newStreamAudioMessage(sampleRate int, pcm []byte) streamAudioMessage {
	if sampleRate <= 0 {
		sampleRate = DefaultSampleRate
	}
	return streamAudioMessage{
		Type: "streamAudio",
		Data: streamAudioData{
			AudioDataType: "raw",
			SampleRate:    sampleRate,
			AudioData:     base64.StdEncoding.EncodeToString(pcm),
		},
	}
}

type clearAudioMessage struct {
	Type string `json:"type"`
}

func newClearAudioMessage() clearAudioMessage {
	return clearAudioMessage{Type: "clearAudio"}
}

func durationMillis(duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(duration) / float64(time.Millisecond)
}
