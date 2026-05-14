package speech

import "context"

type RecognitionKind int

const (
	RecognitionPartial RecognitionKind = iota
	RecognitionFinal
)

type RecognitionEvent struct {
	Kind RecognitionKind
	Text string
}

type STTSession interface {
	WritePCM(ctx context.Context, pcm []byte) error
	Events() <-chan RecognitionEvent
	Close() error
}

type STTProvider interface {
	Start(ctx context.Context, sessionID string) (STTSession, error)
}

type TTSProvider interface {
	Synthesize(ctx context.Context, text string) (<-chan []byte, <-chan error)
}
