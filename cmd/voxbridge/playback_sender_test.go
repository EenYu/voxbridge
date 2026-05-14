package main

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestPCM16ForPlaybackKeeps16kPCM(t *testing.T) {
	in := pcm16Samples(1000, 3000, -1000, -3000, 32767, 32767, -32768, -32768)

	got, err := pcm16ForPlayback(in, 16000)
	if err != nil {
		t.Fatalf("pcm16ForPlayback returned error: %v", err)
	}
	if !bytes.Equal(got, in) {
		t.Fatalf("16k PCM changed: got %v, want %v", pcm16Values(got), pcm16Values(in))
	}
}

func TestPCM16ForPlaybackRejects8kPCM(t *testing.T) {
	in := pcm16Samples(1, -2, 3)

	_, err := pcm16ForPlayback(in, 8000)
	if err == nil || !strings.Contains(err.Error(), "unsupported playback input sample rate") {
		t.Fatalf("error = %v, want unsupported sample rate", err)
	}
}

func TestPCM16ForPlaybackRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name       string
		pcm        []byte
		sampleRate int
		want       string
	}{
		{
			name:       "odd byte length",
			pcm:        []byte{0},
			sampleRate: 16000,
			want:       "odd byte length",
		},
		{
			name:       "unsupported sample rate",
			pcm:        pcm16Samples(10, 20),
			sampleRate: 24000,
			want:       "unsupported playback input sample rate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pcm16ForPlayback(tt.pcm, tt.sampleRate)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestWritePlaybackWAVUses16kHeaderAndOriginalData(t *testing.T) {
	pcm, err := pcm16ForPlayback(pcm16Samples(1000, 3000, -1000, -3000), 16000)
	if err != nil {
		t.Fatalf("pcm16ForPlayback returned error: %v", err)
	}

	var wav bytes.Buffer
	if err := writePCM16WAV(&wav, pcm, playbackSampleRate); err != nil {
		t.Fatalf("writePCM16WAV returned error: %v", err)
	}
	data := wav.Bytes()
	if len(data) != 44+len(pcm) {
		t.Fatalf("WAV length = %d, want %d", len(data), 44+len(pcm))
	}
	if got := string(data[0:4]); got != "RIFF" {
		t.Fatalf("chunk id = %q, want RIFF", got)
	}
	if got := string(data[8:12]); got != "WAVE" {
		t.Fatalf("format = %q, want WAVE", got)
	}
	if got := binary.LittleEndian.Uint16(data[20:22]); got != 1 {
		t.Fatalf("audio format = %d, want 1", got)
	}
	if got := binary.LittleEndian.Uint16(data[22:24]); got != 1 {
		t.Fatalf("channels = %d, want 1", got)
	}
	if got := binary.LittleEndian.Uint32(data[24:28]); got != playbackSampleRate {
		t.Fatalf("sample rate = %d, want %d", got, playbackSampleRate)
	}
	if got := binary.LittleEndian.Uint32(data[28:32]); got != playbackSampleRate*2 {
		t.Fatalf("byte rate = %d, want %d", got, playbackSampleRate*2)
	}
	if got := binary.LittleEndian.Uint16(data[32:34]); got != 2 {
		t.Fatalf("block align = %d, want 2", got)
	}
	if got := binary.LittleEndian.Uint16(data[34:36]); got != 16 {
		t.Fatalf("bits per sample = %d, want 16", got)
	}
	if got := string(data[36:40]); got != "data" {
		t.Fatalf("data chunk id = %q, want data", got)
	}
	if got := binary.LittleEndian.Uint32(data[40:44]); got != uint32(len(pcm)) {
		t.Fatalf("data size = %d, want %d", got, len(pcm))
	}
	if got := data[44:]; !bytes.Equal(got, pcm) {
		t.Fatalf("WAV PCM = %v, want %v", pcm16Values(got), pcm16Values(pcm))
	}
}

func pcm16Samples(values ...int16) []byte {
	pcm := make([]byte, len(values)*2)
	for i, value := range values {
		binary.LittleEndian.PutUint16(pcm[i*2:], uint16(value))
	}
	return pcm
}

func pcm16Values(pcm []byte) []int16 {
	values := make([]int16, len(pcm)/2)
	for i := range values {
		values[i] = int16(binary.LittleEndian.Uint16(pcm[i*2:]))
	}
	return values
}
