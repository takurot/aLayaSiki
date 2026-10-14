# MCP agent interface

`alayasiki-mcp` exposes the standard runtime through MCP protocol `2024-11-05`.
Local clients use newline-delimited JSON-RPC on stdio; the authenticated HTTP API
also hosts the legacy SSE transport. This implementation does not claim support
for the newer Streamable HTTP transport.

## Local stdio

```sh
cargo run -p alayasiki-mcp -- --data-dir /path/to/agent-db
```

The default database directory is `$HOME/.alayasiki-mcp` (`USERPROFILE` on Windows).
`ALAYASIKI_DATA_DIR` or `--data-dir` overrides it. Help/version commands exit before
opening a database. Only protocol messages go to stdout. EOF closes the connection,
joins the worker at a job boundary and flushes the repository.

The local process is trusted through OS permissions: its principal has the `admin`
role, with tenant `local` and subject `local-agent`. `ALAYASIKI_MCP_TENANT` and
`ALAYASIKI_MCP_SUBJECT` select a different local identity. Never open a data directory
concurrently in another process; use remote SSE to share one server-owned database.
Local MCP and HTTP have different default directories.

Initialization requires `protocolVersion`, `capabilities`, and `clientInfo`:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"example","version":"1"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
```

Notifications receive no response. Ping, discovery, resource reads and prompts use
standard JSON-RPC request/result/error envelopes. Tool results contain a text block
with a JSON value; execution failures return `isError: true` with sanitized messages.

## Tools

| Name | Arguments | Behavior |
|---|---|---|
| `alayasiki_query` | Full query DSL, including `query`, modes, filters and snapshot | Authorized GraphRAG with evidence/citations |
| `alayasiki_ingest` | `content`, optional metadata/idempotency key, or canonical `request` | Authorized autonomous ingestion |
| `alayasiki_session_write` | `session_id`, content/metadata or canonical request | Owner-scoped temporary working memory |
| `alayasiki_session_read` | `session_id` | Read the caller's unexpired graph |

Example:

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"alayasiki_ingest","arguments":{"content":"A Rust startup acquired a company.","metadata":{"source":"notes.md"},"idempotency_key":"notes-1"}}}
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"alayasiki_query","arguments":{"query":"What happened to the startup?","search_mode":"local","mode":"answer"}}}
```

Canonical file requests use the same externally tagged `Text`/`File` shape as
[the HTTP API](API.md). Persistent ingestion supports existing PDF extraction and
image/audio metadata hints. Working memory accepts bounded text/Markdown/JSON or
media hints; PDF documents use persistent ingestion to avoid unbounded expansion.

Session graphs expire after the existing 30-minute TTL and do not survive process
restart. Ownership combines tenant and subject; another identity cannot read or
write the graph. IDs should be unique within the shared repository.

## Resources and prompts

- `alayasiki://communities`: current caller-tenant community summaries, excluding
  retention-expired data before summarization.
- `alayasiki://sessions/{session_id}`: owner-scoped temporary graph resource.
- `alayasiki_reasoning` prompt with required `question`: a template for grounding
  reasoning in evidence and citations.

Community resources rebuild from current tenant-visible data; query summaries keep
their existing immutable snapshot cache and historical semantics.

## Authenticated HTTP/SSE

Start `alayasiki-api` with the JWT configuration in [API.md](API.md). Connect to
`GET /mcp/sse` with `Authorization: Bearer <token>`. The first SSE `endpoint` event
announces `/mcp/messages?session_id=<UUID>`. POST JSON-RPC messages to that endpoint
with the bearer token; the HTTP status is 202 and replies arrive as SSE `message`
events. SSE session IDs are transport state, distinct from working-memory IDs.

Each POST is authenticated again and uses the current JWT roles/scopes. Its tenant
and subject must match the opening connection. Browser Origin headers are rejected
by default; `ALAYASIKI_MCP_ALLOWED_ORIGINS` configures an exact comma-separated
allowlist. Keep local binding on loopback and use a TLS reverse proxy remotely.

The SSE server caps connections at 128, queued replies at 32 per connection and
concurrent dispatches at 32. It reserves output capacity before executing a mutation;
backpressure returns 429 before any write. Disconnect releases transport state.
Shutdown rejects new connections, closes streams and drains dispatch before runtime
shutdown. In-flight durable operations are not canceled by HTTP disconnect.

## Bounds and verification

Protocol frames and replies are limited to 8 MiB. Shared working-memory accounting
survives SSE reconnects and conservatively caps 128 graphs, 4096 writes and 16 MiB
of serialized input. A single working-memory write permits 64 KiB content and 4 KiB
metadata. Expired graph objects are reclaimed before their budget is reused.
If ingestion fails after writing (for example, a strict audit failure), retained
working memory remains charged. Failed attempts against an existing graph may be
conservatively overcounted until TTL expiry rather than allowing unaccounted data.
These safety bounds complement the HTTP admission limit; per-tenant billing/SLM
cost policies remain Issue #66.

```sh
cargo test -p alayasiki-mcp
cargo test -p alayasiki-api --test http_test
```

Tests cover dispatcher discovery/execution, session ownership, tenant communities,
protocol errors, stdio subprocess framing/EOF, real SSE handshake/tools, current
JWT permissions, origins, reconnects, queue saturation and TTL memory accounting.
EOF cancellation of clients does not imply durable background jobs were all finished;
pending jobs replay on restart.
