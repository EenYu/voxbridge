package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"voxbridge/internal/freeswitch"
	"voxbridge/internal/llm"
	"voxbridge/internal/media"
	"voxbridge/internal/playback"
	"voxbridge/internal/speech"
)

type gatedSTTProvider struct {
	mu sync.Mutex

	calls         int
	firstStarted  chan struct{}
	secondStarted chan struct{}
	allowFirst    chan struct{}
}

func newGatedSTTProvider() *gatedSTTProvider {
	return &gatedSTTProvider{
		firstStarted:  make(chan struct{}),
		secondStarted: make(chan struct{}),
		allowFirst:    make(chan struct{}),
	}
}

func (p *gatedSTTProvider) Start(context.Context, string) (speech.STTSession, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()

	switch call {
	case 1:
		close(p.firstStarted)
		<-p.allowFirst
	case 2:
		close(p.secondStarted)
	}

	return &gatedSTTSession{events: make(chan speech.RecognitionEvent)}, nil
}

type gatedSTTSession struct {
	mu     sync.Mutex
	closed bool
	events chan speech.RecognitionEvent
}

func (s *gatedSTTSession) WritePCM(context.Context, []byte) error { return nil }

func (s *gatedSTTSession) Events() <-chan speech.RecognitionEvent { return s.events }

func (s *gatedSTTSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.events)
	return nil
}

type noopTTSProvider struct{}

func (noopTTSProvider) Synthesize(context.Context, string) (<-chan []byte, <-chan error) {
	chunks := make(chan []byte)
	errs := make(chan error)
	close(chunks)
	close(errs)
	return chunks, errs
}

type noopLLM struct{}

func (noopLLM) StreamChat(context.Context, []llm.Message) (<-chan llm.Delta, <-chan error) {
	deltas := make(chan llm.Delta)
	errs := make(chan error)
	close(deltas)
	close(errs)
	return deltas, errs
}

type discardSender struct{}

func (discardSender) SendPCM(context.Context, []byte) error { return nil }

func (discardSender) ClearQueued() {}

func TestSessionRegistrySerializesConcurrentNewByUUID(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stt := newGatedSTTProvider()
	registry := newSessionRegistry(stt, noopTTSProvider{}, noopLLM{}, nil, 16000, playback.ModeWSBinary, logger)

	type result struct {
		session media.DialogSession
		err     error
	}

	firstCh := make(chan result, 1)
	secondCh := make(chan result, 1)
	info := media.ConnectionInfo{CallUUID: "call-1"}

	go func() {
		session, err := registry.New(context.Background(), info, discardSender{})
		firstCh <- result{session: session, err: err}
	}()

	select {
	case <-stt.firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first session creation did not start")
	}

	go func() {
		session, err := registry.New(context.Background(), info, discardSender{})
		secondCh <- result{session: session, err: err}
	}()

	select {
	case <-stt.secondStarted:
		t.Fatal("second session creation started before the first one released the call lock")
	case <-time.After(100 * time.Millisecond):
	}

	close(stt.allowFirst)

	var first result
	select {
	case first = <-firstCh:
	case <-time.After(time.Second):
		t.Fatal("first session creation did not finish")
	}
	if first.err != nil {
		t.Fatalf("first New() error = %v", first.err)
	}

	select {
	case <-stt.secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second session creation never started after the first completed")
	}

	var second result
	select {
	case second = <-secondCh:
	case <-time.After(time.Second):
		t.Fatal("second session creation did not finish")
	}
	if second.err != nil {
		t.Fatalf("second New() error = %v", second.err)
	}
	defer second.session.Close()

	if err := first.session.WritePCM(context.Background(), []byte{0, 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("replaced session WritePCM() error = %v, want %v", err, context.Canceled)
	}

	current, ok := second.session.(*trackedSession)
	if !ok {
		t.Fatalf("second session type = %T, want *trackedSession", second.session)
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.sessions) != 1 {
		t.Fatalf("len(registry.sessions) = %d, want 1", len(registry.sessions))
	}
	if registry.sessions[info.CallUUID] != current {
		t.Fatal("registry did not keep the latest session for the call UUID")
	}
}

func TestSessionRegistryAudioSenderUsesFallbackInUUIDBroadcastMode(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := newSessionRegistry(nil, nil, nil, &freeswitch.Client{}, 16000, playback.ModeUUIDBroadcast, logger)

	sender := registry.audioSenderFor(media.ConnectionInfo{CallUUID: "call-1"}, discardSender{})
	if _, ok := sender.(*freeSwitchAudioSender); !ok {
		t.Fatalf("sender type = %T, want *freeSwitchAudioSender", sender)
	}
}

func TestSessionRegistryAudioSenderKeepsWebSocketSenderInWSBinaryMode(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := newSessionRegistry(nil, nil, nil, &freeswitch.Client{}, 16000, playback.ModeWSBinary, logger)

	original := discardSender{}
	sender := registry.audioSenderFor(media.ConnectionInfo{CallUUID: "call-1"}, original)
	if _, ok := sender.(discardSender); !ok {
		t.Fatalf("sender type = %T, want discardSender", sender)
	}
}
