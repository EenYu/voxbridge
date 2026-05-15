package dialog

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"voxbridge/internal/llm"
	"voxbridge/internal/speech"
)

type fakeSTTProvider struct {
	session *fakeSTTSession
}

func (p *fakeSTTProvider) Start(context.Context, string) (speech.STTSession, error) {
	p.session = &fakeSTTSession{events: make(chan speech.RecognitionEvent, 8)}
	return p.session, nil
}

type fakeSTTSession struct {
	events chan speech.RecognitionEvent
}

func (s *fakeSTTSession) WritePCM(context.Context, []byte) error { return nil }
func (s *fakeSTTSession) Events() <-chan speech.RecognitionEvent { return s.events }
func (s *fakeSTTSession) Close() error {
	close(s.events)
	return nil
}

type blockingLLM struct {
	mu       sync.Mutex
	canceled int
	calls    int
}

func (b *blockingLLM) StreamChat(ctx context.Context, _ []llm.Message) (<-chan llm.Delta, <-chan error) {
	deltas := make(chan llm.Delta, 1)
	errs := make(chan error)
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	go func() {
		defer close(deltas)
		defer close(errs)
		deltas <- llm.Delta{Content: "你好。"}
		<-ctx.Done()
		b.mu.Lock()
		b.canceled++
		b.mu.Unlock()
	}()
	return deltas, errs
}

type blockingTTS struct {
	started  chan struct{}
	canceled chan struct{}
}

func (t *blockingTTS) Synthesize(ctx context.Context, _ string) (<-chan []byte, <-chan error) {
	chunks := make(chan []byte)
	errs := make(chan error)
	go func() {
		defer close(chunks)
		defer close(errs)
		chunks <- []byte{0, 1, 2, 3}
		if t.started != nil {
			close(t.started)
		}
		<-ctx.Done()
		close(t.canceled)
	}()
	return chunks, errs
}

type recordingSender struct {
	mu        sync.Mutex
	clears    int
	sentBytes int
	sentPCM   [][]byte
}

func (s *recordingSender) SendPCM(_ context.Context, pcm []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sentBytes++
	s.sentPCM = append(s.sentPCM, append([]byte(nil), pcm...))
	return nil
}

func (s *recordingSender) ClearQueued() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clears++
}

type blockingClearSender struct {
	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

func (s *blockingClearSender) SendPCM(context.Context, []byte) error { return nil }

func (s *blockingClearSender) ClearQueued() {
	s.startOnce.Do(func() {
		close(s.started)
	})
	<-s.release
}

func (s *blockingClearSender) Release() {
	s.releaseOnce.Do(func() {
		close(s.release)
	})
}

type startedLLM struct {
	started chan struct{}
	once    sync.Once
}

func (s *startedLLM) StreamChat(ctx context.Context, _ []llm.Message) (<-chan llm.Delta, <-chan error) {
	deltas := make(chan llm.Delta)
	errs := make(chan error)
	s.once.Do(func() {
		close(s.started)
	})
	go func() {
		defer close(deltas)
		defer close(errs)
		select {
		case <-ctx.Done():
		case deltas <- llm.Delta{Done: true}:
		}
	}()
	return deltas, errs
}

type channelSender struct {
	sent chan []byte
}

func (s *channelSender) SendPCM(ctx context.Context, pcm []byte) error {
	out := append([]byte(nil), pcm...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.sent <- out:
		return nil
	}
}

func (s *channelSender) ClearQueued() {}

type holdingChunkTTS struct {
	chunk []byte
}

func (t *holdingChunkTTS) Synthesize(ctx context.Context, _ string) (<-chan []byte, <-chan error) {
	chunks := make(chan []byte)
	errs := make(chan error)
	go func() {
		defer close(chunks)
		defer close(errs)
		select {
		case <-ctx.Done():
			return
		case chunks <- append([]byte(nil), t.chunk...):
		}
		<-ctx.Done()
	}()
	return chunks, errs
}

type lockedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *lockedBuffer) BytesCopy() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.Buffer.Bytes()...)
}

func TestSessionCancelsActiveResponseOnBargeIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stt := &fakeSTTProvider{}
	chat := &blockingLLM{}
	tts := &blockingTTS{started: make(chan struct{}), canceled: make(chan struct{})}
	sender := &recordingSender{}

	session, err := NewSession(ctx, "call-1", stt, tts, chat, sender, Options{MaxTTSChars: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stt.session.events <- speech.RecognitionEvent{Kind: speech.RecognitionFinal, Text: "你好"}
	select {
	case <-tts.started:
	case <-time.After(time.Second):
		t.Fatal("expected TTS to start")
	}

	stt.session.events <- speech.RecognitionEvent{Kind: speech.RecognitionPartial, Text: "等一下"}

	select {
	case <-tts.canceled:
	case <-time.After(time.Second):
		t.Fatal("expected active TTS context to be canceled")
	}

	eventually(t, time.Second, func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		return sender.clears >= 1
	})
}

func TestBlockingClearQueuedDoesNotBlockFinalTurnLLM(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stt := &fakeSTTProvider{}
	chat := &startedLLM{started: make(chan struct{})}
	tts := &recordingTTS{}
	sender := &blockingClearSender{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer sender.Release()

	session, err := NewSession(ctx, "call-1", stt, tts, chat, sender, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stt.session.events <- speech.RecognitionEvent{Kind: speech.RecognitionFinal, Text: "开始"}

	select {
	case <-chat.started:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("blocking ClearQueued should not block LLM start")
	}

	sender.Release()
	select {
	case <-sender.started:
	case <-time.After(time.Second):
		t.Fatal("expected ClearQueued to be scheduled")
	}
}

type scriptedLLM struct {
	deltas []llm.Delta
}

func (s *scriptedLLM) StreamChat(ctx context.Context, _ []llm.Message) (<-chan llm.Delta, <-chan error) {
	deltas := make(chan llm.Delta)
	errs := make(chan error)
	go func() {
		defer close(deltas)
		defer close(errs)
		for _, delta := range s.deltas {
			select {
			case <-ctx.Done():
				return
			case deltas <- delta:
			}
		}
	}()
	return deltas, errs
}

type recordingTTS struct {
	mu    sync.Mutex
	texts []string
}

func (t *recordingTTS) Synthesize(ctx context.Context, text string) (<-chan []byte, <-chan error) {
	chunks := make(chan []byte, 1)
	errs := make(chan error)
	t.mu.Lock()
	t.texts = append(t.texts, text)
	t.mu.Unlock()
	go func() {
		defer close(chunks)
		defer close(errs)
		select {
		case chunks <- []byte(text):
		case <-ctx.Done():
		}
	}()
	return chunks, errs
}

func TestSessionStreamsFinalRecognitionThroughLLMAndTTS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stt := &fakeSTTProvider{}
	chat := &scriptedLLM{deltas: []llm.Delta{
		{Content: "好的"},
		{Content: "，马上处理。"},
		{Done: true},
	}}
	tts := &recordingTTS{}
	sender := &recordingSender{}

	session, err := NewSession(ctx, "call-1", stt, tts, chat, sender, Options{MaxTTSChars: 40})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stt.session.events <- speech.RecognitionEvent{Kind: speech.RecognitionFinal, Text: "帮我查一下余额"}

	eventually(t, time.Second, func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		return sender.sentBytes > 0
	})
	eventually(t, time.Second, func() bool {
		tts.mu.Lock()
		defer tts.mu.Unlock()
		return len(tts.texts) > 0 && tts.texts[0] == "好的，马上处理。"
	})
}

func TestRecognitionFinalUsesEventTimeInTurnSummary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var logs lockedBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	asrFinalAt := time.Now().Add(-250 * time.Millisecond)
	stt := &fakeSTTProvider{}
	chat := &scriptedLLM{deltas: []llm.Delta{
		{Content: "好。", Done: true},
	}}
	tts := &recordingTTS{}
	sender := &recordingSender{}

	session, err := NewSession(ctx, "call-1", stt, tts, chat, sender, Options{
		MaxTTSChars: 40,
		Logger:      logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stt.session.events <- speech.RecognitionEvent{
		Kind:       speech.RecognitionFinal,
		Text:       "开始",
		ReceivedAt: asrFinalAt,
	}

	eventually(t, time.Second, func() bool {
		return bytes.Contains(logs.BytesCopy(), []byte(`"msg":"turn latency summary"`))
	})
	summary := findLogRecord(t, logs.BytesCopy(), "turn latency summary")
	newField := int64LogAttr(t, summary, "asr_final_to_llm_first_token_ms")
	compatField := int64LogAttr(t, summary, "final_asr_to_llm_first_token_ms")
	if newField < 150 {
		t.Fatalf("asr_final_to_llm_first_token_ms = %d, want event-time based latency", newField)
	}
	if compatField != newField {
		t.Fatalf("final_asr_to_llm_first_token_ms = %d, want %d", compatField, newField)
	}
}

func TestFirstTTSPlaybackChunkUsesTwentyMSBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stt := &fakeSTTProvider{}
	chat := &scriptedLLM{deltas: []llm.Delta{
		{Content: "好。", Done: true},
	}}
	tts := &holdingChunkTTS{
		chunk: bytes.Repeat([]byte{0, 1}, defaultFirstTTSPlaybackChunkSize/2),
	}
	sender := &channelSender{sent: make(chan []byte, 1)}

	session, err := NewSession(ctx, "call-1", stt, tts, chat, sender, Options{MaxTTSChars: 40})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stt.session.events <- speech.RecognitionEvent{Kind: speech.RecognitionFinal, Text: "开始"}

	select {
	case pcm := <-sender.sent:
		if len(pcm) != defaultFirstTTSPlaybackChunkSize {
			t.Fatalf("first SendPCM bytes = %d, want %d", len(pcm), defaultFirstTTSPlaybackChunkSize)
		}
		if len(pcm)%2 != 0 {
			t.Fatalf("first SendPCM bytes = %d, want even PCM16 length", len(pcm))
		}
	case <-time.After(time.Second):
		t.Fatal("expected first 20ms PCM chunk before TTS stream finished")
	}
}

type signalingLLM struct {
	deltas []llm.Delta
	sent   chan struct{}
}

func (s *signalingLLM) StreamChat(ctx context.Context, _ []llm.Message) (<-chan llm.Delta, <-chan error) {
	deltas := make(chan llm.Delta)
	errs := make(chan error)
	go func() {
		defer close(deltas)
		defer close(errs)
		defer close(s.sent)
		for _, delta := range s.deltas {
			select {
			case <-ctx.Done():
				return
			case deltas <- delta:
			}
		}
	}()
	return deltas, errs
}

func TestSessionPipelineReadsLLMWhileTTSBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stt := &fakeSTTProvider{}
	chat := &signalingLLM{
		deltas: []llm.Delta{
			{Content: "第一句。"},
			{Content: "第二句。"},
			{Done: true},
		},
		sent: make(chan struct{}),
	}
	tts := &blockingTTS{started: make(chan struct{}), canceled: make(chan struct{})}
	sender := &recordingSender{}

	session, err := NewSession(ctx, "call-1", stt, tts, chat, sender, Options{MaxTTSChars: 40})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stt.session.events <- speech.RecognitionEvent{Kind: speech.RecognitionFinal, Text: "开始"}
	select {
	case <-tts.started:
	case <-time.After(time.Second):
		t.Fatal("expected TTS to start")
	}
	select {
	case <-chat.sent:
	case <-time.After(time.Second):
		t.Fatal("expected LLM stream to be consumed while TTS is blocked")
	}
}

func TestSessionSendsFragmentsInOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stt := &fakeSTTProvider{}
	chat := &scriptedLLM{deltas: []llm.Delta{
		{Content: "A。"},
		{Content: "B。"},
		{Content: "C。"},
		{Done: true},
	}}
	tts := &recordingTTS{}
	sender := &recordingSender{}

	session, err := NewSession(ctx, "call-1", stt, tts, chat, sender, Options{MaxTTSChars: 40})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	stt.session.events <- speech.RecognitionEvent{Kind: speech.RecognitionFinal, Text: "按顺序说"}

	eventually(t, time.Second, func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		return len(sender.sentPCM) == 3
	})

	sender.mu.Lock()
	got := make([]string, 0, len(sender.sentPCM))
	for _, pcm := range sender.sentPCM {
		got = append(got, string(pcm))
	}
	sender.mu.Unlock()
	want := []string{"A。", "B。", "C。"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sentPCM[%d] = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
}

func TestShouldFlushTTS(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		maxChars int
		minChars int
		want     bool
	}{
		{name: "sentence punctuation", text: "好的。", maxChars: 10, minChars: 6, want: true},
		{name: "length", text: "一二三四五", maxChars: 5, minChars: 10, want: true},
		{name: "short", text: "你好", maxChars: 10, minChars: 6, want: false},
		{name: "short weak punctuation", text: "好的，", maxChars: 10, minChars: 6, want: false},
		{name: "long weak punctuation", text: "这是一个较长的停顿，", maxChars: 20, minChars: 6, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldFlushTTS(tt.text, tt.maxChars, tt.minChars); got != tt.want {
				t.Fatalf("shouldFlushTTS()=%v want %v", got, tt.want)
			}
		})
	}
}

func TestTrimHistoryKeepsSystemPromptAndRecentUserTurns(t *testing.T) {
	history := []llm.Message{
		{Role: llm.RoleSystem, Content: "sys"},
		{Role: llm.RoleUser, Content: "u1"},
		{Role: llm.RoleAssistant, Content: "a1"},
		{Role: llm.RoleUser, Content: "u2"},
		{Role: llm.RoleAssistant, Content: "a2"},
		{Role: llm.RoleUser, Content: "u3"},
	}

	got := trimHistory(history, 2)
	want := []llm.Message{
		{Role: llm.RoleSystem, Content: "sys"},
		{Role: llm.RoleUser, Content: "u2"},
		{Role: llm.RoleAssistant, Content: "a2"},
		{Role: llm.RoleUser, Content: "u3"},
	}

	if len(got) != len(want) {
		t.Fatalf("len(trimHistory()) = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("trimHistory()[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestTurnMetricsMarkersAreIdempotent(t *testing.T) {
	turn := newTurnMetrics(1, time.Now())

	if !turn.markLLMFirstToken() || turn.markLLMFirstToken() {
		t.Fatal("markLLMFirstToken should only succeed once")
	}
	if !turn.markFirstFragment() || turn.markFirstFragment() {
		t.Fatal("markFirstFragment should only succeed once")
	}
	if !turn.markTTSFirstAudio() || turn.markTTSFirstAudio() {
		t.Fatal("markTTSFirstAudio should only succeed once")
	}
	if !turn.markFirstPlaybackQueued() || turn.markFirstPlaybackQueued() {
		t.Fatal("markFirstPlaybackQueued should only succeed once")
	}
}

func TestTurnMetricsDefaultsMissingASRFinalTime(t *testing.T) {
	before := time.Now()
	turn := newTurnMetrics(1, time.Time{})
	after := time.Now()

	if turn.asrFinalAt.IsZero() {
		t.Fatal("asrFinalAt should default when missing")
	}
	if turn.asrFinalAt.Before(before) || turn.asrFinalAt.After(after) {
		t.Fatalf("asrFinalAt = %v, want between %v and %v", turn.asrFinalAt, before, after)
	}
}

func findLogRecord(t *testing.T, data []byte, msg string) map[string]any {
	t.Helper()
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("unmarshal log record: %v\n%s", err, line)
		}
		if record["msg"] == msg {
			return record
		}
	}
	t.Fatalf("log record %q not found in:\n%s", msg, data)
	return nil
}

func int64LogAttr(t *testing.T, record map[string]any, key string) int64 {
	t.Helper()
	value, ok := record[key]
	if !ok {
		t.Fatalf("log attr %q not found in %#v", key, record)
	}
	number, ok := value.(float64)
	if !ok {
		t.Fatalf("log attr %q = %#v, want number", key, value)
	}
	return int64(number)
}

func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
