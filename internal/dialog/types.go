package dialog

import "context"

type AudioSender interface {
	SendPCM(ctx context.Context, pcm []byte) error
	ClearQueued()
}
