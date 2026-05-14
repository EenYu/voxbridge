package freeswitch

import "testing"

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
