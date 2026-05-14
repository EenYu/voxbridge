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
	wavPath := flag.String("wav", "", "optional 16k mono PCM WAV file to send")
	flag.Parse()

	provider, err := volcengine.NewSTTProvider(volcengine.STTConfig{
		Endpoint:   os.Getenv("VOXBRIDGE_VOLCENGINE_STT_ENDPOINT"),
		AppID:      os.Getenv("VOXBRIDGE_VOLCENGINE_APP_ID"),
		Token:      os.Getenv("VOXBRIDGE_VOLCENGINE_TOKEN"),
		AccessKey:  os.Getenv("VOXBRIDGE_VOLCENGINE_ACCESS_KEY"),
		ResourceID: os.Getenv("VOXBRIDGE_VOLCENGINE_STT_RESOURCE_ID"),
		Audio: volcengine.AudioConfig{
			Format:   "pcm",
			Codec:    "raw",
			Rate:     16000,
			Bits:     16,
			Channel:  1,
			Language: "zh-CN",
		},
		Timeout:   15 * time.Second,
		ChunkSize: 3200,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "configure ASR: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	session, err := provider.Start(ctx, "voxbridge-asr-probe")
	if err != nil {
		fmt.Fprintf(os.Stderr, "start ASR: %v\n", err)
		os.Exit(1)
	}

	audio := make([]byte, 16000*2)
	if *wavPath != "" {
		audio, err = readPCM16WAV(*wavPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read wav: %v\n", err)
			os.Exit(1)
		}
		audio = append(audio, make([]byte, 16000*2*3/2)...)
	}
	if err := writeAudio(ctx, session, audio); err != nil {
		fmt.Fprintf(os.Stderr, "write ASR audio: %v\n", err)
		os.Exit(1)
	}

	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-session.Events():
			if !ok {
				if err := session.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "close ASR after events closed: %v\n", err)
					os.Exit(1)
				}
				fmt.Println("ok connected_sent_pcm events_closed=true")
				return
			}
			fmt.Printf("event kind=%d text_len=%d\n", ev.Kind, len([]rune(ev.Text)))
			if ev.Kind == 1 {
				_ = session.Close()
				fmt.Println("ok connected_sent_pcm final=true")
				return
			}
		case <-timer.C:
			if err := session.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "close ASR after timeout: %v\n", err)
				os.Exit(1)
			}
			fmt.Println("ok connected_sent_pcm no_text_for_silence=true")
			return
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "wait ASR: %v\n", ctx.Err())
			os.Exit(1)
		}
	}
}

type pcmWriter interface {
	WritePCM(context.Context, []byte) error
}

func writeAudio(ctx context.Context, session pcmWriter, pcm []byte) error {
	const chunkSize = 16000 * 2 / 5
	for start := 0; start < len(pcm); start += chunkSize {
		end := start + chunkSize
		if end > len(pcm) {
			end = len(pcm)
		}
		if err := session.WritePCM(ctx, pcm[start:end]); err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

func readPCM16WAV(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	header := make([]byte, 12)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, err
	}
	if string(header[:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return nil, fmt.Errorf("not a WAV file")
	}
	for {
		var chunkID [4]byte
		if _, err := io.ReadFull(file, chunkID[:]); err != nil {
			return nil, err
		}
		var size uint32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			return nil, err
		}
		if string(chunkID[:]) == "data" {
			pcm := make([]byte, size)
			_, err := io.ReadFull(file, pcm)
			return pcm, err
		}
		if _, err := file.Seek(int64(size), io.SeekCurrent); err != nil {
			return nil, err
		}
	}
}
