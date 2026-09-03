# QuickDrop compatibility

QuickDrop uses semantic versions for repositories and an explicit integer for
the wire protocol. Repository versions do not need to be identical; protocol
support is the compatibility boundary.

| Component line | Go SDK | Peer protocol | Server API/signaling |
| --- | --- | --- | --- |
| Web 1.x | Go SDK 0.1.x / 0.2.x | v1 | Server 0.1.x |
| CLI 0.1.x | Go SDK 0.1.x | v1 | Server 0.1.x |
| CLI 0.2.x | Go SDK 0.2.x | v1 | Server 0.1.x |

Within protocol v1, receivers ignore unknown optional JSON fields and unknown
control message types. Binary framing, ordering, authentication semantics, or
required fields cannot change without protocol v2 and capability negotiation.

Release order:

1. Publish `quickdrop-go` and tag the SDK version required by the CLI.
2. Update and verify server/CLI dependency checksums against that tag.
3. Publish `quickdrop-server` and `quickdrop-cli`.
4. Deploy the server and web independently; old v1 peers remain compatible.

The local parent workspace may use `go.work` before publication. Relative
`replace` directives must never be committed to release repositories.
