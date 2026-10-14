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

## Install with uvx

Platform wheels let [uv](https://docs.astral.sh/uv/) run the native Rust server
without a Rust toolchain or Python extension bindings:

```sh
uvx alayasiki-mcp --version
uvx alayasiki-mcp --data-dir /path/to/agent-db
```

For reproducible client configurations, pin a release with
`uvx --from alayasiki-mcp==0.3.0 alayasiki-mcp`. The wheel installs the native
`alayasiki-mcp` executable directly on PATH through maturin's `bin` bindings.
There is no Python wrapper: `[project.scripts]` is intended for Python
`module:function` entrypoints and is unnecessary for this native executable.
See [maturin binary bindings](https://www.maturin.rs/bindings).

Wheels cover Linux x86_64/aarch64 (glibc 2.28 or newer), macOS Intel/Apple Silicon,
and Windows x86_64. Alpine/musl wheels are not included in this release matrix.
Python 3.10 or newer and an installed `uv` are required; `uv` can manage Python.

### Claude Desktop and other local MCP clients

Add this entry to the client's MCP configuration (`claude_desktop_config.json`
for Claude Desktop), then restart the client:

```json
{
  "mcpServers": {
    "alayasiki": {
      "command": "uvx",
      "args": ["alayasiki-mcp"]
    }
  }
}
```

If a desktop process does not inherit your shell PATH, use the absolute path of
`uvx` for `command`. The default database is the user's `.alayasiki-mcp` directory.
To select a separate database or identity, add an optional `env` object under the
server entry:

```json
{
  "ALAYASIKI_DATA_DIR": "/absolute/path/to/agent-db",
  "ALAYASIKI_MCP_TENANT": "my-project",
  "ALAYASIKI_MCP_SUBJECT": "desktop-agent"
}
```

The packaged command runs stdio locally. To share an existing database remotely,
use an MCP client supporting the authenticated legacy SSE transport described
above at `/mcp/sse`; the API process owns the shared database. Never point two
stdio processes at the same directory.

## Build and release wheels

Package metadata lives in `mcp/pyproject.toml`; the root `pyproject.toml` continues
to describe benchmark dependencies. Build a platform wheel from the MCP directory:

```sh
cd mcp
maturin build --release --locked --out ../dist
cd ..
python scripts/smoke_mcp_wheel.py -- uvx --from /absolute/path/to/dist/alayasiki_mcp-0.3.0-py3-none-PLATFORM.whl alayasiki-mcp
```

The smoke test checks help/version and a real stdio lifecycle: initialize,
tool discovery, persistent ingest, evidence-backed query, working-memory write,
and read. It uses a temporary database and requires stdout to contain only JSON-RPC.

`.github/workflows/publish-mcp.yml` builds and installs every native wheel on pull
requests and `mcp-v*` tags. Manual dispatch builds/tests only. All five installed
wheels must pass smoke tests before a tag-triggered publishing job can run.
The tag `mcp-v0.3.0` must match both Cargo package and Python project versions.
Builds use ordinary portable target settings, not `target-cpu=native`.

Publication uses PyPI Trusted Publishing (OIDC), with no stored API token. The
publisher must match repository `takurot/aLayaSiki`, workflow `publish-mcp.yml`, and
GitHub environment `pypi`. This trust is configured in PyPI separately from GitHub;
a pending publisher creates the PyPI project on its first successful upload.
Only a tag push can publish; pull requests, forks, and manual builds cannot.
See [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/).

Before pushing a release tag, review the packaging change, wait for all required
CI and wheel checks, then tag that exact tested commit. After publication verify
`uvx --from alayasiki-mcp==0.3.0 alayasiki-mcp --version` and run the smoke test
against that public package. Publication is complete only after those checks pass.
