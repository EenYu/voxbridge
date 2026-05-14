package volcengine

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestLiveTTS(t *testing.T) {
	if os.Getenv("VOXBRIDGE_LIVE_VOLCENGINE_TTS") != "1" {
		t.Skip("set VOXBRIDGE_LIVE_VOLCENGINE_TTS=1 to run live Volcengine TTS check")
	}

	provider, err := NewTTSProvider(TTSConfig{
		Endpoint:   os.Getenv("VOXBRIDGE_VOLCENGINE_TTS_ENDPOINT"),
		AppID:      os.Getenv("VOXBRIDGE_VOLCENGINE_APP_ID"),
		Token:      os.Getenv("VOXBRIDGE_VOLCENGINE_TOKEN"),
		AccessKey:  os.Getenv("VOXBRIDGE_VOLCENGINE_ACCESS_KEY"),
		ResourceID: os.Getenv("VOXBRIDGE_VOLCENGINE_TTS_RESOURCE_ID"),
		VoiceType:  os.Getenv("VOXBRIDGE_VOLCENGINE_TTS_VOICE_TYPE"),
		Audio: AudioConfig{
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
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	chunks, errs := provider.Synthesize(ctx, "你好，这是 VoxBridge 语音合成测试。")
	var bytes int
	for chunks != nil || errs != nil {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				chunks = nil
				continue
			}
			bytes += len(chunk)
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if bytes == 0 {
		t.Fatal("TTS returned no audio")
	}
	t.Logf("received %d PCM bytes", bytes)
}
