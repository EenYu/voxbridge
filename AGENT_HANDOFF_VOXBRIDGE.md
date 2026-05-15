# VoxBridge Integration Handoff

## 1. Current State

`mod_audio_duplex` PoC is implemented and proven on the FreeSWITCH host.

Validated on server:

- Host: `10.130.44.125`
- FreeSWITCH module path: `/usr/lib/freeswitch/mod/mod_audio_duplex.so`
- FreeSWITCH API command: `uuid_audio_duplex`
- Existing VoxBridge service: `voxbridge.service`
- VoxBridge binary: `/usr/local/bin/voxbridge`
- VoxBridge env file: `/etc/voxbridge/voxbridge.env`
- VoxBridge health check: `curl http://127.0.0.1:18080/healthz`

Known-good validation already completed:

- FreeSWITCH -> backend raw PCM16 binary streaming works
- Backend -> FreeSWITCH raw PCM16 binary playback works
- Playback reaches the caller on a parked call
- `clearAudio` control is received and applied

Important note:

- Test route `9998` was temporarily added only for module validation.
- Production route `9999` has not yet been switched to `mod_audio_duplex`.
- Existing `9999` behavior and VoxBridge service are still the baseline production path.

## 2. FreeSWITCH Contract

### Command

Start:

```text
uuid_audio_duplex <uuid> start <ws-url> mono 16000 <metadata-json>
```

Stop:

```text
uuid_audio_duplex <uuid> stop
```

Example:

```text
uuid_audio_duplex 9082b9b4-9c3f-4fc5-beae-e94ba8808fbc start ws://127.0.0.1:18080/fs/media mono 16000 {"route":"9999"}
```

### Audio/protocol behavior

One WebSocket per call.

FreeSWITCH -> backend:

1. Sends text JSON:

```json
{
  "type": "stream.start",
  "uuid": "<uuid>",
  "sampleRate": 16000,
  "channels": 1,
  "encoding": "pcm_s16le",
  "metadata": {}
}
```

2. Then sends caller audio as WebSocket binary frames.
3. Payload format is `pcm_s16le`, mono, `16000 Hz`.
4. Typical frame cadence is `20 ms`.

Backend -> FreeSWITCH:

- Preferred playback input is WebSocket binary frames
- Format must be `pcm_s16le`, mono, `16000 Hz`
- Backend can send `20-100 ms` chunks
- Backend may burst faster than realtime; module buffers and plays at media clock pace

Control message supported:

```json
{"type":"clearAudio"}
```

Compatibility path also supported:

```json
{
  "type": "streamAudio",
  "data": {
    "audioDataType": "raw",
    "sampleRate": 16000,
    "audioData": "<base64 pcm>"
  }
}
```

`clearAudio` clears pending assistant playback from the in-memory buffer. No temp WAV files are used.

## 3. Module Behavior Relevant To VoxBridge

- The module internally forces `send_silence_when_idle=-1` on the channel when duplex audio starts.
- This is required so parked/idle calls still produce write-side media frames and playback injection works.
- Playback is injected through FreeSWITCH `WRITE_REPLACE`.
- Current implementation is mono-only and targets `16000 Hz` external media.
- FreeSWITCH channel audio can still be `8000 Hz`; the module resamples internally.

## 4. Required VoxBridge Changes

The VoxBridge agent should switch playback from file-based `uuid_broadcast` to WebSocket binary playback when configured.

### Config

Add a config switch:

```text
VOXBRIDGE_PLAYBACK_MODE=uuid_broadcast|ws_binary
```

Recommended initial rollout:

```text
VOXBRIDGE_PLAYBACK_MODE=ws_binary
```

only when dialplan is explicitly using `mod_audio_duplex`.

### Startup / session control

Where VoxBridge currently starts media streaming with `uuid_audio_stream`, switch to:

```text
uuid_audio_duplex <uuid> start <VOXBRIDGE_PUBLIC_WS_URL> mono 16000 <metadata-json>
```

and stop with:

```text
uuid_audio_duplex <uuid> stop
```

Keep the old `uuid_audio_stream` + `uuid_broadcast` path as fallback during rollout.

### Media handler

In the WebSocket media handler used by FreeSWITCH:

- keep receiving inbound caller audio exactly as now
- for assistant playback, stop wrapping PCM in JSON/base64 by default
- send assistant PCM directly as `websocket.BinaryMessage`
- use roughly `20-100 ms` chunking

For barge-in / cancel:

- current behavior that clears queued playback should map to:

```json
{"type":"clearAudio"}
```

over the same WebSocket.

### Existing VoxBridge touchpoints

These were the original intended integration files:

- `cmd/voxbridge/main.go`
- `internal/media/handler.go`
- `internal/media/audio.go`
- `internal/dialog/session.go`
- `internal/freeswitch/client.go`
- `cmd/voxbridge/playback_sender.go`

Expected direction:

- `playback_sender.go` becomes fallback-only
- WebSocket handler becomes the primary assistant playback path in `ws_binary` mode
- session cancel/break logic should send `clearAudio`

## 5. Runtime Wiring On 10.130.44.125

Current production env already contains:

```text
VOXBRIDGE_PUBLIC_WS_URL=ws://127.0.0.1:18080/fs/media
VOXBRIDGE_AUDIO_SAMPLE_RATE_HZ=16000
VOXBRIDGE_AUDIO_CHANNELS=1
VOXBRIDGE_AUDIO_CODEC=pcm_s16le
VOXBRIDGE_AUDIO_FRAME_MS=20
```

This matches `mod_audio_duplex` expectations and should be reused.

The intended steady-state production topology is:

1. Caller dials `9999`
2. FreeSWITCH answers/parks call
3. VoxBridge detects the call event as it does today
4. VoxBridge starts:

```text
uuid_audio_duplex <uuid> start ws://127.0.0.1:18080/fs/media mono 16000 <metadata>
```

5. VoxBridge receives caller PCM from the same WebSocket
6. VoxBridge sends assistant PCM back as binary frames on the same WebSocket
7. On barge-in/cancel, VoxBridge sends `{"type":"clearAudio"}`

## 6. Acceptance Criteria

The VoxBridge integration is complete when all of the following are true on `9999`:

- No assistant playback path uses temp WAV files
- No `uuid_broadcast` is used in `ws_binary` mode
- Caller audio still reaches ASR/LLM/TTS end-to-end
- Assistant audio is heard smoothly by the caller
- Barge-in causes prompt playback stop through `clearAudio`
- Hangup or backend disconnect does not crash FreeSWITCH
- Fallback mode `uuid_broadcast` can still be enabled if needed

## 7. Recommended Rollout Plan

1. Keep current `9999` path untouched until VoxBridge binary supports `ws_binary`
2. Add `VOXBRIDGE_PLAYBACK_MODE`
3. Change VoxBridge to issue `uuid_audio_duplex start/stop`
4. Change assistant playback from JSON/base64/file path to binary WebSocket PCM
5. Use `clearAudio` for barge-in
6. Test on a non-production extension first
7. Only then switch `9999`

## 8. Operational Notes

- `mod_audio_duplex` is already buildable and loadable on `10.130.44.125`
- If needed, verify module presence with:

```bash
fs_cli -x "module_exists mod_audio_duplex"
fs_cli -x "help" | grep uuid_audio_duplex
```

- Current temporary test harness / watcher used during PoC validation should not be treated as production components
- VoxBridge should be the long-term WebSocket peer, not the Python test harness
