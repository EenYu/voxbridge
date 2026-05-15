# VoxBridge

VoxBridge is a Go MVP service that connects FreeSWITCH calls to an AI voice dialog pipeline:

- FreeSWITCH ESL starts `mod_audio_duplex` for answered calls by default.
- `/fs/media` receives 16 kHz mono L16 PCM WebSocket audio.
- Volcengine STT emits partial/final recognition events.
- An OpenAI-compatible Chat Completions endpoint streams assistant text.
- Volcengine TTS produces PCM chunks that are sent back as WebSocket binary PCM in `ws_binary` mode.
- Barge-in cancels the active LLM/TTS response when new speech is recognized.

## Run

```bash
go test ./...
go run ./cmd/voxbridge
```

Minimal environment:

```bash
export VOXBRIDGE_HTTP_ADDR=:8080
export VOXBRIDGE_ESL_ADDRESS=127.0.0.1:8021
export VOXBRIDGE_ESL_PASSWORD=ClueCon
export VOXBRIDGE_PUBLIC_WS_URL=ws://YOUR_PUBLIC_HOST:8080/fs/media
export VOXBRIDGE_PLAYBACK_MODE=ws_binary

export VOXBRIDGE_LLM_BASE_URL=https://api.openai.com
export VOXBRIDGE_LLM_API_KEY=YOUR_LLM_KEY
export VOXBRIDGE_LLM_MODEL=gpt-4.1-mini

export VOXBRIDGE_VOLCENGINE_APP_ID=YOUR_APP_ID
export VOXBRIDGE_VOLCENGINE_ACCESS_KEY=YOUR_ACCESS_KEY
export VOXBRIDGE_VOLCENGINE_RESOURCE_ID=YOUR_RESOURCE_ID
export VOXBRIDGE_VOLCENGINE_TTS_VOICE_TYPE=zh_female_wanwanxiaohe_moon_bigtts
```

Optional Volcengine overrides:

```bash
export VOXBRIDGE_VOLCENGINE_STT_ENDPOINT=wss://openspeech.bytedance.com/api/v3/sauc/bigmodel
export VOXBRIDGE_VOLCENGINE_TTS_ENDPOINT=wss://openspeech.bytedance.com/api/v3/tts/bidirection
export VOXBRIDGE_VOLCENGINE_STT_RESOURCE_ID=...
export VOXBRIDGE_VOLCENGINE_TTS_RESOURCE_ID=...
export VOXBRIDGE_VOLCENGINE_STT_CLUSTER=...
export VOXBRIDGE_VOLCENGINE_TTS_CLUSTER=...
export VOXBRIDGE_VOLCENGINE_STT_ASYNC_END_WINDOW_SIZE=600
export VOXBRIDGE_VOLCENGINE_STT_ASYNC_FORCE_TO_SPEECH_TIME=800
```

The async STT sentence-boundary settings are used only when the STT endpoint is
the Volcengine `bigmodel_async` endpoint. Non-async STT endpoints keep the
streaming request shape unchanged.

## FreeSWITCH

The playback mode is controlled by `VOXBRIDGE_PLAYBACK_MODE`:

- `ws_binary` (default): `uuid_audio_duplex` + outbound binary PCM over the same WebSocket
- `uuid_broadcast`: legacy `uuid_audio_stream` + temp WAV + `uuid_broadcast`

In `ws_binary` mode the ESL loop listens for `CHANNEL_ANSWER` and executes:

```text
uuid_audio_duplex <uuid> start <public-ws-url> mono 16k <metadata>
```

In `uuid_broadcast` mode it executes:

```text
uuid_audio_stream <uuid> start <public-ws-url>?uuid=<uuid> mono 16k <metadata>
```

The WebSocket media endpoint expects binary inbound PCM. In `ws_binary` mode it sends outbound raw PCM as `websocket.BinaryMessage`, chunked to `20-100 ms` frames. `clearAudio` is still sent as JSON:

```json
{"type":"clearAudio"}
```

Legacy compatibility output for `uuid_broadcast` mode remains:

```json
{
  "type": "streamAudio",
  "data": {
    "audioDataType": "raw",
    "sampleRate": 16000,
    "audioData": "<base64-pcm>"
  }
}
```
