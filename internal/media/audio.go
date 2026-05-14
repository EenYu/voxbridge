package media

import (
	"encoding/base64"
	"time"
)

const (
	DefaultSampleRate     = 16000
	DefaultChannels       = 1
	DefaultBytesPerSample = 2
	DefaultChunkDuration  = 150 * time.Millisecond
	MinChunkDuration      = 100 * time.Millisecond
	MaxChunkDuration      = 200 * time.Millisecond
)

// ChunkSizeBytes returns a PCM chunk size aligned to a complete audio frame.
// The requested duration is clamped to the 100-200ms range expected by the
// FreeSWITCH media websocket path.
func ChunkSizeBytes(sampleRate, channels, bytesPerSample int, target time.Duration) int {
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
		target = DefaultChunkDuration
	}
	if target < MinChunkDuration {
		target = MinChunkDuration
	}
	if target > MaxChunkDuration {
		target = MaxChunkDuration
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
