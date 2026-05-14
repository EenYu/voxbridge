package llm

import "context"

const (
	RoleSystem    = "system"
	RoleUser      = "user"
	RoleAssistant = "assistant"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Delta struct {
	Content string
	Done    bool
}

type Client interface {
	StreamChat(ctx context.Context, messages []Message) (<-chan Delta, <-chan error)
}
