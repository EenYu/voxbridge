package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type OpenAIOptions struct {
	BaseURL    string
	APIKey     string
	Model      string
	HTTPClient *http.Client
	Timeout    time.Duration
}

type OpenAIClient struct {
	baseURL    string
	apiKey     string
	model      string
	httpClient *http.Client
}

func NewOpenAICompatibleClient(opts OpenAIOptions) (*OpenAIClient, error) {
	if strings.TrimSpace(opts.BaseURL) == "" {
		return nil, errors.New("llm base URL is required")
	}
	if strings.TrimSpace(opts.Model) == "" {
		return nil, errors.New("llm model is required")
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		timeout := opts.Timeout
		if timeout <= 0 {
			timeout = 60 * time.Second
		}
		httpClient = &http.Client{Timeout: timeout}
	}

	return &OpenAIClient{
		baseURL:    strings.TrimRight(opts.BaseURL, "/"),
		apiKey:     strings.TrimSpace(opts.APIKey),
		model:      strings.TrimSpace(opts.Model),
		httpClient: httpClient,
	}, nil
}

func (c *OpenAIClient) StreamChat(ctx context.Context, messages []Message) (<-chan Delta, <-chan error) {
	deltas := make(chan Delta)
	errs := make(chan error, 1)

	go func() {
		defer close(deltas)
		defer close(errs)

		err := c.streamChat(ctx, messages, func(delta Delta) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case deltas <- delta:
				return nil
			}
		})
		if err != nil {
			errs <- err
		}
	}()

	return deltas, errs
}

func (c *OpenAIClient) streamChat(ctx context.Context, messages []Message, emit func(Delta) error) error {
	body, err := json.Marshal(chatCompletionRequest{
		Model:    c.model,
		Messages: messages,
		Stream:   true,
	})
	if err != nil {
		return fmt.Errorf("marshal chat completion request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create chat completion request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send chat completion request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("chat completion request failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	return parseChatCompletionSSE(resp.Body, emit)
}

type chatCompletionRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
	Stream   bool      `json:"stream"`
}

type chatCompletionChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

func parseChatCompletionSSE(r io.Reader, emit func(Delta) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024), 1024*1024)

	var dataLines []string
	doneEmitted := false
	emitDone := func() error {
		if doneEmitted {
			return nil
		}
		doneEmitted = true
		return emit(Delta{Done: true})
	}
	flush := func() error {
		if len(dataLines) == 0 {
			return nil
		}
		payload := strings.Join(dataLines, "\n")
		dataLines = nil

		if payload == "[DONE]" {
			return emitDone()
		}

		var chunk chatCompletionChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			return fmt.Errorf("parse chat completion SSE payload: %w", err)
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.Content != "" {
				if err := emit(Delta{Content: choice.Delta.Content}); err != nil {
					return err
				}
			}
			if choice.FinishReason != nil {
				if err := emitDone(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read chat completion SSE: %w", err)
	}
	return flush()
}
