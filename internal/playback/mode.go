package playback

import "strings"

type Mode string

const (
	ModeUUIDBroadcast Mode = "uuid_broadcast"
	ModeWSBinary      Mode = "ws_binary"
)

func Normalize(mode Mode) Mode {
	return Mode(strings.TrimSpace(string(mode)))
}

func IsValid(mode Mode) bool {
	switch Normalize(mode) {
	case ModeUUIDBroadcast, ModeWSBinary:
		return true
	default:
		return false
	}
}
