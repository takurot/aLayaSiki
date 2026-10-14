# Live graph explorer

Start the authenticated API server using [API.md](API.md). Then:

```sh
cd ui
npm ci
npm run dev
```

Paste an access token with the API's issuer/audience and a query-capable role into
the explorer's **Access token** field, then select **Connect**. The token stays in
memory; it is not written to browser storage or bundled into the application.
Disconnect clears the graph, selected details and chat. Connecting as a different
identity cancels pending requests and starts a fresh view.

The development server proxies same-origin `/v1` and `/health` requests to
`http://127.0.0.1:8080`. Set `ALAYASIKI_API_TARGET` when starting Vite to choose a
different API backend. In production, serve the built UI and reverse proxy `/v1`
under one HTTPS origin. The existing production CSP allows same-origin connections;
no permissive cross-origin policy or embedded JWT is required.

## Exploring and asking questions

The graph loads up to 1000 tenant-visible nodes and edges. A notice indicates when
the initial page reaches the node limit. Refresh reloads the graph and clears selected details. Selecting
a node opens its metadata/provenance; graph controls also support keyboard use.
Empty datasets, loading and API errors are shown explicitly.

Chat sends real local GraphRAG requests with depth 2 and top_k 20. Replies display
answer text, evidence, citations, model/snapshot and confidence/groundedness where
available. **View evidence** displays the returned subgraph even when its nodes are
outside the initial graph page. Node IDs are preserved as decimal strings, including
Rust u64 values above JavaScript's safe integer range.

The backend currently uses deterministic embeddings and lightweight rule extraction
until the semantic/SLM roadmap tasks are implemented; the UI reflects real backend
results and does not simulate generated replies.

## Validation

```sh
npm test -- --coverage
npm run lint
npm run build
npx playwright install chromium
npm run test:e2e
```

Vitest/React Testing Library tests run in `ui-quality` CI with 80% coverage thresholds.
The Playwright test starts a real API server in a temporary data directory and a
Vite server, ingests through HTTP, connects using a test-only JWT, and checks graph,
chat/evidence and disconnect behavior. It uses isolated ports 18080/4173 and runs
in the `UI Live API E2E` job.

Dependency checks distinguish production dependencies from development tooling.
The runtime dependency audit is clean. Seven existing Tailwind 3 toolchain audit
entries require an upstream braces fix or Tailwind 4 migration; this separate build
system migration is recorded in PLAN rather than mixed into live API integration.
