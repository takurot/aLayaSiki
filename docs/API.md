# HTTP API and remote SDK

The `alayasiki-api` binary uses the standard autonomous runtime, including durable
extraction and snapshot-compatible community summaries. It binds to
`127.0.0.1:8080` by default. Set `ALAYASIKI_BIND` to override the address and
`ALAYASIKI_DATA_DIR` to choose the durable data directory (default: `data`).

## Starting the server

Configure these environment variables through your secret manager or shell:

- `ALAYASIKI_JWT_SECRET`: HS256 secret, at least 32 bytes.
- `ALAYASIKI_JWT_ISSUER`: required JWT issuer.
- `ALAYASIKI_JWT_AUDIENCE`: required JWT audience.

Then run `cargo run -p alayasiki-api`. Missing/invalid configuration fails startup.
For remote production access, terminate TLS at a reverse proxy and proxy the API
under the same origin as the explorer UI. The server does not issue JWTs: your
identity provider/operator signs them with the configured issuer and audience.

Claims include `sub`, `tenant`, `exp`, `iss`, `aud`, and `roles` or `scope`. Default
roles are `reader` (query), `ingestor` (ingest) and `admin` (both plus metrics).
Supported scopes include `query:execute`, `ingest:write`, and `admin:*`.
The authenticated claim determines ownership; request metadata cannot select a
foreign tenant. Persistent deduplication keys and entity IDs are tenant-scoped.
Extracted entities inherit the earliest retention deadline among their contributing
sources; later unbounded or longer-lived sources cannot extend that deadline. This
conservative policy can hide a shared entity earlier than a surviving source until
source-aware entity resolution is implemented.

## Routes

| Route | Access | Response data |
|---|---|---|
| `GET /health` | Public | Minimal status |
| `POST /v1/ingest` | Ingest | `node_ids`, durable `snapshot_id` |
| `POST /v1/query` | Query | `QueryResponse` with evidence, citations and explain plan |
| `GET /v1/graph?limit=100&after=0` | Query | Tenant-visible nodes, edges and `next_after` cursor |
| `GET /metrics` | Admin | Query metrics snapshot |

Supply `Authorization: Bearer <token>` on protected routes. Global admission is
bounded to 64 simultaneous requests and 100 requests per second; JSON bodies are
limited to 8 MiB. Library `ServerConfig` exposes these limits for embedding. Full
per-tenant quotas and SLM cost controls remain Issue #66.

Responses use `{ "success": true, "data": ... }` or
`{ "success": false, "error": { "code": "...", "message": "...", "retryable": false } }`.
Error codes follow the core `ErrorCode` model. Internal/authentication details are
not returned to clients. Backpressure and transient failures identify safe retries.

### Ingestion

The canonical JSON body uses serde's externally tagged `IngestionRequest`:

```json
{
  "Text": {
    "content": "A Rust startup acquired a company.",
    "metadata": { "source": "example.md" },
    "idempotency_key": "document-1"
  }
}
```

For files use `File` with `filename`, `mime_type`, `content` (a byte array),
`metadata`, and an optional `idempotency_key`. Text, Markdown, JSON and PDF extraction
reuse the ingestion library. Image metadata must contain `ocr_text`, `caption`,
`alt_text` or `description`; audio metadata must contain `transcript`, `caption` or
`description`. Raw image/audio inference remains Issue #101. `embedding_model_id`
and `extraction_model_id` are optional; the deprecated `model_id` aliases embedding
only. Automatic entity extraction is asynchronous and uses the existing lightweight
rule models until Issue #48 supplies actual SLM inference.

### Query

```json
{
  "query": "What is the startup's strategy?",
  "mode": "answer",
  "search_mode": "local",
  "traversal": { "depth": 2 },
  "top_k": 20
}
```

All existing DSL filters, `snapshot_id`, `time_travel` and owner-scoped session
queries are supported. Query IDs are serialized as Rust `u64` JSON integers; JavaScript
clients must parse them losslessly. Graph endpoint IDs and pagination cursors are
decimal strings. The graph endpoint caps `limit` at 1000 and filters expired records;
only edges whose endpoints occur in the returned visible page are included.

## Remote Rust SDK

```rust
use alayasiki_sdk::{ClientBuilder, RetryConfig};

let client = ClientBuilder::new()
    .retry_config(RetryConfig::default())
    .connect_remote("http://127.0.0.1:8080", &access_token)?;
// client.ingest(IngestionRequest::text(...)).await?;
// client.query(QueryRequest { ... }).await?;
```

`RemoteTransport` sends one attempt; `Client` supplies the same jittered retry policy
and overall deadline used by in-process clients. Ingestion retries require an
idempotency key by default. Authentication and permanent errors are not retried.
Redirects are disabled, credentials are excluded from diagnostics, and responses
are limited to 8 MiB. Use HTTPS outside local development.

## Audit and shutdown

The binary persists operation audit events to `audit.jsonl`, and HTTP authentication
denials to `http-audit.jsonl`, under the data directory. JWTs and request bodies are
not included in these records. Configured audit failure is fail-closed.

Ctrl-C stops admission and drains HTTP requests and detached durable operations
before joining the extraction worker and flushing the repository. Client disconnect
cannot cancel an in-progress WAL mutation. Restart recovers graph/snapshot data and
pending durable extraction jobs. Only one process should open a data directory.
