package media

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

func TestChunkSizeBytesClampsAndAlignsToL16Frames(t *testing.T) {
	tests := []struct {
		name   string
		target time.Duration
		want   int
	}{
		{name: "default", target: 0, want: 4800},
		{name: "clamps low", target: 50 * time.Millisecond, want: 3200},
		{name: "keeps target", target: 125 * time.Millisecond, want: 4000},
		{name: "clamps high", target: 500 * time.Millisecond, want: 6400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ChunkSizeBytes(16000, 1, 2, tt.target)
			if got != tt.want {
				t.Fatalf("ChunkSizeBytes() = %d, want %d", got, tt.want)
			}
			if got%2 != 0 {
				t.Fatalf("ChunkSizeBytes() = %d, want L16 frame alignment", got)
			}
		})
	}
}

func TestStreamAudioMessageBase64JSON(t *testing.T) {
	pcm := []byte{0x00, 0x01, 0x02, 0xff}
	encoded, err := json.Marshal(newStreamAudioMessage(16000, pcm))
	if err != nil {
		t.Fatal(err)
	}

	var got struct {
		Type string `json:"type"`
		Data struct {
			AudioDataType string `json:"audioDataType"`
			SampleRate    int    `json:"sampleRate"`
			AudioData     string `json:"audioData"`
		} `json:"data"`
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}

	if got.Type != "streamAudio" {
		t.Fatalf("type = %q, want streamAudio", got.Type)
	}
	if got.Data.AudioDataType != "raw" {
		t.Fatalf("audioDataType = %q, want raw", got.Data.AudioDataType)
	}
	if got.Data.SampleRate != 16000 {
		t.Fatalf("sampleRate = %d, want 16000", got.Data.SampleRate)
	}
	if got.Data.AudioData != base64.StdEncoding.EncodeToString(pcm) {
		t.Fatalf("audioData = %q, want base64 PCM", got.Data.AudioData)
	}
}

func TestParseTextEventExtractsMetadataCallUUID(t *testing.T) {
	event := parseTextEvent([]byte(`{
		"type": "metadata",
		"metadata": {
			"call_uuid": "call-123",
			"codec": "L16"
		}
	}`))

	if !event.ShouldStart {
		t.Fatal("ShouldStart = false, want true for metadata event")
	}
	if event.CallUUID != "call-123" {
		t.Fatalf("CallUUID = %q, want call-123", event.CallUUID)
	}
	if event.Metadata["metadata.codec"] != "L16" {
		t.Fatalf("metadata.codec = %q, want L16", event.Metadata["metadata.codec"])
	}
}

func TestParseTextEventStartsOnPlainMetadataUUID(t *testing.T) {
	event := parseTextEvent([]byte(`{
		"event": "CHANNEL_ANSWER",
		"uuid": "call-456"
	}`))
	if !event.ShouldStart {
		t.Fatal("ShouldStart = false, want true when metadata contains uuid")
	}
	if event.CallUUID != "call-456" {
		t.Fatalf("CallUUID = %q, want call-456", event.CallUUID)
	}
}
