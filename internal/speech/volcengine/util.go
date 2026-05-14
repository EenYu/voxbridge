package volcengine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("voxbridge-%d", fallbackNowUnixNano())
	}
	return hex.EncodeToString(b[:])
}

var fallbackNowUnixNano = func() int64 {
	return time.Now().UnixNano()
}

func int32Ptr(v int32) *int32 {
	return &v
}

func absInt32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}

func valueOr(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

func valueOrInt(v, fallback int) int {
	if v != 0 {
		return v
	}
	return fallback
}

func textPreview(text string, maxRunes int) string {
	text = strings.TrimSpace(text)
	if maxRunes <= 0 {
		return text
	}
	runes := []rune(text)
	if len(runes) <= maxRunes {
		return text
	}
	return string(runes[:maxRunes]) + "..."
}

func websocketDialError(prefix string, resp *http.Response, err error) error {
	if resp == nil {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	message := strings.TrimSpace(string(body))
	if message == "" {
		return fmt.Errorf("%s: %w: status %d", prefix, err, resp.StatusCode)
	}
	return fmt.Errorf("%s: %w: status %d: %s", prefix, err, resp.StatusCode, message)
}
