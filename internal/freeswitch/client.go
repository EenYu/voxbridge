package freeswitch

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	defaultReconnectDelay = 3 * time.Second
	defaultStartEvent     = "CHANNEL_ANSWER"
)

type Config struct {
	Address               string
	Password              string
	PublicWSURL           string
	StartEvents           []string
	RequiredVariableName  string
	RequiredVariableValue string
	ReconnectDelay        time.Duration
}

type Client struct {
	cfg    Config
	logger *slog.Logger

	mu   sync.Mutex
	conn *eslConn
}

func NewClient(cfg Config, logger *slog.Logger) (*Client, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, errors.New("freeswitch: missing ESL address")
	}
	if strings.TrimSpace(cfg.Password) == "" {
		return nil, errors.New("freeswitch: missing ESL password")
	}
	if strings.TrimSpace(cfg.PublicWSURL) == "" {
		return nil, errors.New("freeswitch: missing public WebSocket URL")
	}
	if cfg.ReconnectDelay <= 0 {
		cfg.ReconnectDelay = defaultReconnectDelay
	}
	if len(cfg.StartEvents) == 0 {
		cfg.StartEvents = []string{defaultStartEvent}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{cfg: cfg, logger: logger}, nil
}

func (c *Client) Run(ctx context.Context) error {
	for {
		if err := c.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Warn("ESL loop stopped", "error", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.cfg.ReconnectDelay):
		}
	}
}

func (c *Client) StartAudioStream(ctx context.Context, uuid string, metadata map[string]string) error {
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return errors.New("freeswitch: missing UUID")
	}
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata["uuid"] = uuid
	meta, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	wsURL, err := withUUIDQuery(c.cfg.PublicWSURL, uuid)
	if err != nil {
		return err
	}
	cmd := fmt.Sprintf("api uuid_audio_stream %s start %s mono 16000 %s", uuid, wsURL, string(meta))
	_, err = c.command(ctx, cmd)
	return err
}

func (c *Client) StopAudioStream(ctx context.Context, uuid string) error {
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return errors.New("freeswitch: missing UUID")
	}
	_, err := c.command(ctx, fmt.Sprintf("api uuid_audio_stream %s stop", uuid))
	return err
}

func (c *Client) PlayAudio(ctx context.Context, uuid, file string) error {
	uuid = strings.TrimSpace(uuid)
	file = strings.TrimSpace(file)
	if uuid == "" {
		return errors.New("freeswitch: missing UUID")
	}
	if file == "" {
		return errors.New("freeswitch: missing playback file")
	}
	reply, err := c.command(ctx, fmt.Sprintf("api uuid_broadcast %s %s aleg", uuid, file))
	if err != nil {
		c.logger.Warn("freeswitch playback command failed", "uuid", uuid, "file", file, "reply", reply, "error", err)
		return err
	}
	c.logger.Info("freeswitch playback command sent", "uuid", uuid, "file", file, "reply", reply)
	return err
}

func (c *Client) BreakAudio(ctx context.Context, uuid string) error {
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return errors.New("freeswitch: missing UUID")
	}
	reply, err := c.command(ctx, fmt.Sprintf("api uuid_break %s all", uuid))
	if err != nil {
		c.logger.Warn("freeswitch break command failed", "uuid", uuid, "reply", reply, "error", err)
		return err
	}
	c.logger.Info("freeswitch break command sent", "uuid", uuid, "reply", reply)
	return err
}

func (c *Client) runOnce(ctx context.Context) error {
	conn, err := dialESL(ctx, c.cfg.Address, c.cfg.Password)
	if err != nil {
		return err
	}
	defer conn.Close()

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.mu.Unlock()
	}()

	if _, err := conn.Command(ctx, "event plain "+strings.Join(c.cfg.StartEvents, " ")+" CHANNEL_HANGUP CUSTOM"); err != nil {
		return err
	}

	startEvents := map[string]struct{}{}
	for _, name := range c.cfg.StartEvents {
		startEvents[strings.ToUpper(strings.TrimSpace(name))] = struct{}{}
	}

	for {
		frame, err := conn.ReadFrame()
		if err != nil {
			return err
		}
		if !strings.EqualFold(frame.Header.Get("Content-Type"), "text/event-plain") {
			continue
		}
		parsed := parsePlainEventWithBody(frame.Body)
		event := parsed.Header
		eventName := strings.ToUpper(event.Get("Event-Name"))
		if eventName == "CUSTOM" {
			subclass := decodedEventValue(event, "Event-Subclass")
			if strings.EqualFold(subclass, "mod_audio_stream::play") {
				c.handleAudioStreamPlay(ctx, event, parsed.Body)
				continue
			}
			if strings.HasPrefix(subclass, "mod_audio_stream::") {
				c.logger.Info("received mod_audio_stream custom event", "subclass", subclass, "body_bytes", len(parsed.Body))
			}
		}
		if _, ok := startEvents[eventName]; !ok {
			continue
		}
		if !c.matchesRequiredVariable(event) {
			c.logger.Debug("ignoring call event without required channel variable", "event", eventName, "uuid", firstNonEmpty(event.Get("Unique-ID"), event.Get("Channel-Call-UUID")))
			continue
		}
		uuid := firstNonEmpty(event.Get("Unique-ID"), event.Get("Channel-Call-UUID"))
		if uuid == "" {
			continue
		}
		metadata := map[string]string{
			"event":      eventName,
			"caller_id":  event.Get("Caller-Caller-ID-Number"),
			"direction":  event.Get("Call-Direction"),
			"channel_id": uuid,
		}
		if err := c.StartAudioStream(ctx, uuid, metadata); err != nil {
			c.logger.Warn("failed to start audio stream", "uuid", uuid, "error", err)
			continue
		}
		c.logger.Info("started audio stream", "uuid", uuid)
	}
}

func (c *Client) handleAudioStreamPlay(ctx context.Context, event textproto.MIMEHeader, body string) {
	uuid := firstNonEmpty(decodedEventValue(event, "Unique-ID"), decodedEventValue(event, "Channel-Call-UUID"))
	if uuid == "" {
		c.logger.Warn("mod_audio_stream play event missing uuid")
		return
	}
	if strings.TrimSpace(body) == "" {
		body = decodedEventValue(event, "_body")
	}
	var payload struct {
		AudioDataType string `json:"audioDataType"`
		SampleRate    int    `json:"sampleRate"`
		File          string `json:"file"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(body)), &payload); err != nil {
		if decoded, decodeErr := url.QueryUnescape(strings.TrimSpace(body)); decodeErr == nil {
			err = json.Unmarshal([]byte(decoded), &payload)
			body = decoded
		}
		if err != nil {
			c.logger.Warn("mod_audio_stream play event has invalid body", "uuid", uuid, "body", strings.TrimSpace(body), "error", err)
			return
		}
	}
	file := strings.TrimSpace(payload.File)
	if file == "" || !strings.HasPrefix(file, "/") {
		c.logger.Warn("mod_audio_stream play event missing valid file", "uuid", uuid, "file", file)
		return
	}
	commandCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := c.command(commandCtx, fmt.Sprintf("api uuid_broadcast %s %s aleg", uuid, file)); err != nil {
		c.logger.Warn("failed to play mod_audio_stream audio", "uuid", uuid, "file", file, "error", err)
		return
	}
	c.logger.Info("playing mod_audio_stream audio", "uuid", uuid, "file", file, "audio_type", payload.AudioDataType, "sample_rate", payload.SampleRate)
}

func (c *Client) matchesRequiredVariable(event textproto.MIMEHeader) bool {
	name := strings.TrimSpace(c.cfg.RequiredVariableName)
	if name == "" {
		return true
	}
	value := event.Get("variable_" + name)
	if value == "" {
		value = event.Get(name)
	}
	want := strings.TrimSpace(c.cfg.RequiredVariableValue)
	if want == "" {
		return strings.TrimSpace(value) != ""
	}
	return strings.EqualFold(strings.TrimSpace(value), want)
}

func (c *Client) command(ctx context.Context, cmd string) (string, error) {
	conn, err := dialESL(ctx, c.cfg.Address, c.cfg.Password)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.Command(ctx, cmd)
}

type eslConn struct {
	conn net.Conn
	r    *textproto.Reader
	w    *bufio.Writer
	mu   sync.Mutex
}

type frame struct {
	Header textproto.MIMEHeader
	Body   string
}

func dialESL(ctx context.Context, address, password string) (*eslConn, error) {
	dialer := &net.Dialer{}
	raw, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	conn := &eslConn{
		conn: raw,
		r:    textproto.NewReader(bufio.NewReader(raw)),
		w:    bufio.NewWriter(raw),
	}

	greeting, err := conn.ReadFrame()
	if err != nil {
		raw.Close()
		return nil, err
	}
	if strings.EqualFold(greeting.Header.Get("Content-Type"), "auth/request") {
		if _, err := conn.Command(ctx, "auth "+password); err != nil {
			raw.Close()
			return nil, err
		}
	}
	return conn, nil
}

func (c *eslConn) Close() error {
	return c.conn.Close()
}

func (c *eslConn) Command(ctx context.Context, cmd string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if _, err := c.w.WriteString(cmd + "\n\n"); err != nil {
		return "", err
	}
	if err := c.w.Flush(); err != nil {
		return "", err
	}
	frame, err := c.ReadFrame()
	if err != nil {
		return "", err
	}
	reply := strings.TrimSpace(firstNonEmpty(frame.Header.Get("Reply-Text"), frame.Body))
	if strings.HasPrefix(reply, "-ERR") {
		return reply, errors.New(reply)
	}
	return reply, nil
}

func (c *eslConn) ReadFrame() (frame, error) {
	header, err := c.r.ReadMIMEHeader()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return frame{}, err
		}
		return frame{}, fmt.Errorf("read ESL header: %w", err)
	}
	var body string
	if length := header.Get("Content-Length"); length != "" {
		var n int
		if _, err := fmt.Sscanf(length, "%d", &n); err != nil {
			return frame{}, fmt.Errorf("invalid content length %q: %w", length, err)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(c.r.R, buf); err != nil {
			return frame{}, err
		}
		body = string(buf)
	}
	return frame{Header: header, Body: body}, nil
}

func parsePlainEvent(body string) textproto.MIMEHeader {
	return parsePlainEventWithBody(body).Header
}

func decodedEventValue(event textproto.MIMEHeader, key string) string {
	value := event.Get(key)
	if decoded, err := url.QueryUnescape(value); err == nil {
		return decoded
	}
	return value
}

type plainEvent struct {
	Header textproto.MIMEHeader
	Body   string
}

func parsePlainEventWithBody(body string) plainEvent {
	reader := textproto.NewReader(bufio.NewReader(strings.NewReader(body)))
	header, err := reader.ReadMIMEHeader()
	if err != nil {
		return plainEvent{Header: textproto.MIMEHeader{}}
	}
	rest, _ := io.ReadAll(reader.R)
	return plainEvent{Header: header, Body: string(rest)}
}

func withUUIDQuery(rawURL, uuid string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("uuid", uuid)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
