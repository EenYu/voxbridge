package volcengine

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestBuildParseFrameWithGzipAndSequence(t *testing.T) {
	seq := int32(42)
	payload := []byte(`{"hello":"world"}`)
	raw, err := buildFrame(messageTypeFullClientRequest, messageFlagSequence, serializationJSON, compressionGzip, &seq, payload)
	if err != nil {
		t.Fatalf("buildFrame() error = %v", err)
	}

	got, err := parseFrame(raw)
	if err != nil {
		t.Fatalf("parseFrame() error = %v", err)
	}
	if got.MessageType != messageTypeFullClientRequest {
		t.Fatalf("MessageType = %d, want %d", got.MessageType, messageTypeFullClientRequest)
	}
	if got.Sequence != seq {
		t.Fatalf("Sequence = %d, want %d", got.Sequence, seq)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("Payload = %q, want %q", got.Payload, payload)
	}
}

func TestParseErrorFrame(t *testing.T) {
	payload := []byte("bad request")
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(protocolVersion<<4 | headerWords)
	buf.WriteByte(messageTypeError<<4 | messageFlagNone)
	buf.WriteByte(serializationJSON<<4 | compressionNone)
	buf.WriteByte(0)
	_ = binary.Write(buf, binary.BigEndian, uint32(4001))
	_ = binary.Write(buf, binary.BigEndian, uint32(len(payload)))
	buf.Write(payload)

	got, err := parseFrame(buf.Bytes())
	if err != nil {
		t.Fatalf("parseFrame() error = %v", err)
	}
	if got.ErrorCode != 4001 {
		t.Fatalf("ErrorCode = %d, want 4001", got.ErrorCode)
	}
	if string(got.Payload) != string(payload) {
		t.Fatalf("Payload = %q, want %q", got.Payload, payload)
	}
}

func TestParseFrameRejectsBadVersion(t *testing.T) {
	raw, err := buildFrame(messageTypeAudioResult, messageFlagSequence, serializationNone, compressionNone, int32Ptr(1), []byte("pcm"))
	if err != nil {
		t.Fatalf("buildFrame() error = %v", err)
	}
	raw[0] = 0x21
	if _, err := parseFrame(raw); err == nil {
		t.Fatal("parseFrame() error = nil, want error")
	}
}
