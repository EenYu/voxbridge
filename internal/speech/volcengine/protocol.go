package volcengine

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	protocolVersion = 0x01
	headerWords     = 0x01

	messageTypeFullClientRequest = 0x01
	messageTypeAudioOnlyRequest  = 0x02
	messageTypeFullServerResult  = 0x09
	messageTypeAudioResult       = 0x0b
	messageTypeError             = 0x0f

	messageFlagNone        = 0x00
	messageFlagSequence    = 0x01
	messageFlagLast        = 0x02
	messageFlagSequenceEnd = 0x03

	serializationNone = 0x00
	serializationJSON = 0x01

	compressionNone = 0x00
	compressionGzip = 0x01
)

type frame struct {
	MessageType   byte
	MessageFlags  byte
	Serialization byte
	Compression   byte
	Sequence      int32
	ErrorCode     uint32
	Payload       []byte
}

func buildFrame(messageType, flags, serialization, compression byte, sequence *int32, payload []byte) ([]byte, error) {
	if compression == compressionGzip && len(payload) > 0 {
		var err error
		payload, err = gzipBytes(payload)
		if err != nil {
			return nil, err
		}
	}

	buf := bytes.NewBuffer(make([]byte, 0, 8+len(payload)))
	buf.WriteByte(protocolVersion<<4 | headerWords)
	buf.WriteByte(messageType<<4 | flags)
	buf.WriteByte(serialization<<4 | compression)
	buf.WriteByte(0)

	if sequence != nil {
		if err := binary.Write(buf, binary.BigEndian, *sequence); err != nil {
			return nil, err
		}
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(payload))); err != nil {
		return nil, err
	}
	buf.Write(payload)
	return buf.Bytes(), nil
}

func parseFrame(data []byte) (frame, error) {
	if len(data) < 8 {
		return frame{}, fmt.Errorf("volcengine frame too short: %d bytes", len(data))
	}
	if got := data[0] >> 4; got != protocolVersion {
		return frame{}, fmt.Errorf("unsupported volcengine protocol version %d", got)
	}
	headerSize := int(data[0]&0x0f) * 4
	if headerSize < 4 || len(data) < headerSize+4 {
		return frame{}, fmt.Errorf("invalid volcengine header size %d for %d-byte frame", headerSize, len(data))
	}

	f := frame{
		MessageType:   data[1] >> 4,
		MessageFlags:  data[1] & 0x0f,
		Serialization: data[2] >> 4,
		Compression:   data[2] & 0x0f,
	}
	pos := headerSize

	if f.MessageType == messageTypeError {
		if len(data) < pos+8 {
			return frame{}, errors.New("invalid volcengine error frame")
		}
		f.ErrorCode = binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
	} else if f.MessageFlags&messageFlagSequence != 0 {
		if len(data) < pos+8 {
			return frame{}, errors.New("invalid volcengine sequence frame")
		}
		f.Sequence = int32(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
	}

	payloadSize := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if payloadSize < 0 || len(data)-pos < payloadSize {
		return frame{}, fmt.Errorf("invalid volcengine payload size %d for remaining %d bytes", payloadSize, len(data)-pos)
	}
	f.Payload = data[pos : pos+payloadSize]
	if f.Compression == compressionGzip && len(f.Payload) > 0 {
		payload, err := gunzipBytes(f.Payload)
		if err != nil {
			return frame{}, err
		}
		f.Payload = payload
	}
	return f, nil
}

func gzipJSON(v any) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return gzipBytes(payload)
}

func gzipBytes(payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(payload); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzipBytes(payload []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("decompress volcengine payload: %w", err)
	}
	defer zr.Close()
	out, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("read decompressed volcengine payload: %w", err)
	}
	return out, nil
}
