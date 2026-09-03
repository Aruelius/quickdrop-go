# QuickDrop peer protocol v1

This document is the language-neutral contract shared by the browser and the
native CLI. HTTP and WebSocket signaling only establish a peer connection;
file contents are never sent through the QuickDrop HTTP server.

## Signaling

1. `POST /api/sessions` creates a two-peer session and returns a six-digit
   code plus `sessionId`, `peerId`, `peerToken`, and `expiresAt`.
2. `POST /api/sessions/join` with `{ "code": "123456" }` consumes the code.
3. Each peer connects to `/ws` with the three peer credentials in the query.
4. WebSocket messages are JSON objects with `version: 1`, `type`, and an
   optional `payload`. The peer server forwards `offer`, `answer`,
   `ice_candidate`, and transport coordination messages without interpreting
   their WebRTC payloads.
5. The session creator is the offerer and creates the ordered, reliable
   DataChannel labelled `quickdrop-data`. The joining peer answers it.
6. New peers announce `parallel_capability` on `quickdrop-data`. The initiator
   may then negotiate additional independent PeerConnections with reliable,
   unordered `quickdrop-lane-N` DataChannels. A peer that ignores this optional
   message remains fully compatible on the primary connection.

## DataChannel control messages

Control messages are UTF-8 JSON. Unknown message types and unknown object
fields must be ignored so minor protocol extensions remain compatible.

| Type | Required fields | Purpose |
| --- | --- | --- |
| `text` | `id`, `timestamp`, `payload.text` | UTF-8 text message |
| `file_start` | `transferId`, `file` | Offer a file |
| `file_accept` | `transferId` | Permit the sender to stream |
| `file_progress` | `transferId`, `receivedBytes` | Non-durable receiver progress and flow-control acknowledgement |
| `file_ack` | `transferId`, `receivedBytes` | Durable checkpoint or final commit acknowledgement |
| `file_end` | `transferId`, optional `sha256` | Sender reached EOF and publishes the streamed digest |
| `file_cancel` | `transferId`, optional `reason` | Abort a transfer |
| `resume_query` | `transferId`, `file` | Ask whether a partial file is reusable |
| `resume_state` | `transferId`, `receivedBytes` | Resume from an acknowledged byte boundary |

`file` contains `name`, `size`, `mime`, `chunkSize`, `totalChunks`, and
`lastModified`. New senders may also include `sha256`, `commitAck`,
`progressAck`, and `resume`; old receivers ignore them. `commitAck: true` means
the sender supports receiving the final `file_ack(size)` after `file_end` and
destination commit. A receiver must preserve the legacy pre-`file_end` final
acknowledgement when the field
is absent, otherwise older senders would wait indefinitely. A receiver sends
`resume_state` only when `resume: true`; otherwise it restarts at byte zero so
an older sender cannot deadlock on an unknown resume message.

`progressAck: true` advertises that the sender understands `file_progress`.
Without it, receivers preserve the legacy behavior and use `file_ack` for
flow-control progress. `file_progress` allows a new sender to keep a bounded
amount of data in flight without forcing a filesystem sync for every small
window. It must never be treated as resumable progress or final completion.
Only `file_ack` advances the
durable resume boundary, and `file_ack(size)` confirms the verified destination
commit.

The optional transport coordination messages are `parallel_capability`,
`parallel_offer`, `parallel_answer`, and `parallel_ice`. They carry the
negotiated connection count, lane index, SDP, or ICE candidate respectively.
All text and protocol controls remain on `quickdrop-data`; only binary file
packets may be striped across parallel lanes. `channel_capability` optionally
enables reliable cancellation on the negotiated `quickdrop-priority` channel.

## Binary file packet

Every DataChannel binary message is exactly one file chunk:

```text
byte 0      protocol version (1)
byte 1      packet type (1 = file chunk)
bytes 2..5  big-endian uint32 JSON header length
bytes ...   UTF-8 JSON header
bytes ...   file payload
```

The header contains `transferId`, `index`, `offset`, and `length`. Receivers
must validate chunk boundaries, reject duplicates and overlaps, and use a
bounded reorder buffer when parallel lanes deliver chunks out of order. A
missing chunk must fail the transfer after a bounded completion timeout.

## Reliability and filesystems

- The primary DataChannel is ordered and reliable. Extra file lanes may be
  unordered but must remain reliable.
- Senders apply per-channel buffered-amount backpressure and wait for periodic
  `file_progress` (or legacy `file_ack`) messages so application memory is
  bounded.
- Native receivers write to `<name>.quickdrop.part`, sync it, verify the
  optional SHA-256 digest, and atomically rename it only after `file_end`.
- Native receivers periodically sync a durable checkpoint independently of
  their more frequent flow-control progress reports.
- The receiver sends `file_ack(size)` only after final size/hash validation and
  successful destination commit. The sender therefore sends `file_end` before
  waiting for the final acknowledgement.
- A reconnect may reuse a partial file only when its sidecar metadata matches
  the offered name, size, modification time, chunk size, and optional digest.
- File names are always reduced to their final path component. Receivers never
  accept an absolute path or `..` traversal from a peer.

## Compatibility policy

Protocol version 1 remains the baseline for browser/CLI interoperability.
Optional fields and control messages may be added in version 1 only when old
peers can safely ignore them. A change to binary framing, ordering, or security
semantics requires a new protocol version and explicit capability negotiation.
