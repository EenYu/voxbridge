package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"voxbridge/internal/speech/volcengine"
)

func main() {
	outPath := flag.String("out", "", "optional WAV output path")
	text := flag.String("text", "你好，这是 VoxBridge 语音合成测试。", "text to synthesize")
	flag.Parse()

	provider, err := volcengine.NewTTSProvider(volcengine.TTSConfig{
		Endpoint:   os.Getenv("VOXBRIDGE_VOLCENGINE_TTS_ENDPOINT"),
		AppID:      os.Getenv("VOXBRIDGE_VOLCENGINE_APP_ID"),
		Token:      os.Getenv("VOXBRIDGE_VOLCENGINE_TOKEN"),
		AccessKey:  os.Getenv("VOXBRIDGE_VOLCENGINE_ACCESS_KEY"),
		ResourceID: os.Getenv("VOXBRIDGE_VOLCENGINE_TTS_RESOURCE_ID"),
		VoiceType:  os.Getenv("VOXBRIDGE_VOLCENGINE_TTS_VOICE_TYPE"),
		Audio: volcengine.AudioConfig{
			Format:    "pcm",
			Encoding:  "pcm",
			Rate:      16000,
			Bits:      16,
			Channel:   1,
			VoiceType: os.Getenv("VOXBRIDGE_VOLCENGINE_TTS_VOICE_TYPE"),
		},
		Timeout: 15 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "configure TTS: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	chunks, errs := provider.Synthesize(ctx, *text)
	var bytes int
	var audio [][]byte
	for chunks != nil || errs != nil {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				chunks = nil
				continue
			}
			bytes += len(chunk)
			if *outPath != "" {
				audio = append(audio, append([]byte(nil), chunk...))
			}
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "synthesize TTS: %v\n", err)
				os.Exit(1)
			}
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "synthesize TTS: %v\n", ctx.Err())
			os.Exit(1)
		}
	}
	if bytes == 0 {
		fmt.Fprintln(os.Stderr, "synthesize TTS: no audio returned")
		os.Exit(1)
	}
	if *outPath != "" {
		if err := writePCM16WAV(*outPath, audio, 16000); err != nil {
			fmt.Fprintf(os.Stderr, "write wav: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Printf("ok pcm_bytes=%d\n", bytes)
}

func writePCM16WAV(path string, chunks [][]byte, sampleRate int) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	var dataSize uint32
	for _, chunk := range chunks {
		dataSize += uint32(len(chunk))
	}
	byteRate := uint32(sampleRate * 2)
	blockAlign := uint16(2)
	if _, err := io.WriteString(file, "RIFF"); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, uint32(36)+dataSize); err != nil {
		return err
	}
	if _, err := io.WriteString(file, "WAVEfmt "); err != nil {
		return err
	}
	for _, value := range []any{
		uint32(16),
		uint16(1),
		uint16(1),
		uint32(sampleRate),
		byteRate,
		blockAlign,
		uint16(16),
	} {
		if err := binary.Write(file, binary.LittleEndian, value); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(file, "data"); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, dataSize); err != nil {
		return err
	}
	for _, chunk := range chunks {
		if _, err := file.Write(chunk); err != nil {
			return err
		}
	}
	return nil
}
