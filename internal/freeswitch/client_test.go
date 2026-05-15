package freeswitch

import (
	"strings"
	"testing"

	"voxbridge/internal/playback"
)

func TestWithUUIDQuery(t *testing.T) {
	got, err := withUUIDQuery("ws://example.test/fs/media?tenant=a", "abc-123")
	if err != nil {
		t.Fatal(err)
	}
	want := "ws://example.test/fs/media?tenant=a&uuid=abc-123"
	if got != want {
		t.Fatalf("withUUIDQuery()=%q want %q", got, want)
	}
}

func TestParsePlainEvent(t *testing.T) {
	event := parsePlainEvent("Event-Name: CHANNEL_ANSWER\nUnique-ID: call-1\n\n")
	if got := event.Get("Event-Name"); got != "CHANNEL_ANSWER" {
		t.Fatalf("Event-Name=%q", got)
	}
	if got := event.Get("Unique-ID"); got != "call-1" {
		t.Fatalf("Unique-ID=%q", got)
	}
}

func TestParsePlainEventWithBody(t *testing.T) {
	event := parsePlainEventWithBody("Event-Name: CUSTOM\nEvent-Subclass: mod_audio_stream::play\n\n{\"file\":\"/tmp/a.r16\"}")
	if got := event.Header.Get("Event-Name"); got != "CUSTOM" {
		t.Fatalf("Event-Name=%q", got)
	}
	if got := event.Header.Get("Event-Subclass"); got != "mod_audio_stream::play" {
		t.Fatalf("Event-Subclass=%q", got)
	}
	if got := event.Body; got != "{\"file\":\"/tmp/a.r16\"}" {
		t.Fatalf("Body=%q", got)
	}
}

func TestMatchesRequiredVariable(t *testing.T) {
	client := &Client{cfg: Config{RequiredVariableName: "voxbridge_enabled", RequiredVariableValue: "true"}}
	matching := parsePlainEvent("variable_voxbridge_enabled: true\n\n")
	if !client.matchesRequiredVariable(matching) {
		t.Fatal("expected required variable to match")
	}
	nonMatching := parsePlainEvent("variable_voxbridge_enabled: false\n\n")
	if client.matchesRequiredVariable(nonMatching) {
		t.Fatal("expected required variable mismatch")
	}
}

func TestBuildStartAudioStreamCommandUsesDuplexWithoutUUIDQueryInWSBinaryMode(t *testing.T) {
	client := &Client{cfg: Config{
		PublicWSURL:  "ws://127.0.0.1:18080/fs/media",
		PlaybackMode: playback.ModeWSBinary,
	}}

	got, err := client.buildStartAudioStreamCommand("call-1", map[string]string{"route": "9999"})
	if err != nil {
		t.Fatalf("buildStartAudioStreamCommand() error = %v", err)
	}
	if !strings.Contains(got, "api uuid_audio_duplex call-1 start ws://127.0.0.1:18080/fs/media mono 16000 ") {
		t.Fatalf("command = %q", got)
	}
	if strings.Contains(got, "?uuid=") {
		t.Fatalf("command = %q, should not append uuid query in ws_binary mode", got)
	}
	if !strings.Contains(got, `"uuid":"call-1"`) || !strings.Contains(got, `"route":"9999"`) {
		t.Fatalf("command metadata missing expected JSON: %q", got)
	}
}

func TestBuildStartAudioStreamCommandUsesAudioStreamAndUUIDQueryInFallbackMode(t *testing.T) {
	client := &Client{cfg: Config{
		PublicWSURL:  "ws://127.0.0.1:18080/fs/media?tenant=a",
		PlaybackMode: playback.ModeUUIDBroadcast,
	}}

	got, err := client.buildStartAudioStreamCommand("call-2", map[string]string{"route": "9999"})
	if err != nil {
		t.Fatalf("buildStartAudioStreamCommand() error = %v", err)
	}
	if !strings.Contains(got, "api uuid_audio_stream call-2 start ws://127.0.0.1:18080/fs/media?tenant=a&uuid=call-2 mono 16000 ") {
		t.Fatalf("command = %q", got)
	}
}

func TestBuildStopAudioStreamCommandSwitchesByPlaybackMode(t *testing.T) {
	duplex := &Client{cfg: Config{PlaybackMode: playback.ModeWSBinary}}
	if got := duplex.buildStopAudioStreamCommand("call-1"); got != "api uuid_audio_duplex call-1 stop" {
		t.Fatalf("duplex stop command = %q", got)
	}

	stream := &Client{cfg: Config{PlaybackMode: playback.ModeUUIDBroadcast}}
	if got := stream.buildStopAudioStreamCommand("call-1"); got != "api uuid_audio_stream call-1 stop" {
		t.Fatalf("stream stop command = %q", got)
	}
}
