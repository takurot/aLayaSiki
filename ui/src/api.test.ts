import { describe, it, expect, vi } from "vitest";
import { ApiClient } from "./api";
describe("API client", () => {
  it("preserves u64 evidence identifiers", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response(
            '{"success":true,"data":{"answer":"Rust","evidence":{"nodes":[{"id":18446744073709551614,"data":"Rust","score":0.7,"hop":1,"confidence":0.8,"provenance":{"source":"test.md"}}],"edges":[]},"citations":[{"node_id":18446744073709551614,"source":"test.md","span":[0,4],"confidence":0.8}],"model_id":"test","snapshot_id":"wal-lsn-2","groundedness":0.8,"latency_ms":3}}',
          ),
        ),
    );
    const result = await new ApiClient("token").query(
      "Rust",
      new AbortController().signal,
    );
    expect(result.evidence.nodes[0].id).toBe("18446744073709551614");
    expect(result.citations[0].node_id).toBe("18446744073709551614");
    expect(result.evidence.nodes[0].score).toBe(0.7);
  });
  it("reports server errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response(
            '{"success":false,"error":{"code":"UNAUTHENTICATED","message":"Authentication required"}}',
            { status: 401 },
          ),
        ),
    );
    await expect(
      new ApiClient("token").graph(new AbortController().signal),
    ).rejects.toThrow("Authentication required");
  });
});
it("normalizes graph and sends authorized canonical query", async () => {
  const fetcher = vi.fn().mockResolvedValue(
    new Response(
      JSON.stringify({
        success: true,
        data: {
          nodes: [{ id: "42", data: "Rust", metadata: { source: "test.md" } }],
          edges: [{ source: "42", target: "42", relation: "mentions" }],
          next_after: "42",
        },
      }),
    ),
  );
  vi.stubGlobal("fetch", fetcher);
  expect(
    (await new ApiClient("token").graph(new AbortController().signal)).edges[0]
      .relation_type,
  ).toBe("mentions");
  expect(fetcher).toHaveBeenCalledWith(
    "/v1/graph?limit=1000",
    expect.objectContaining({ headers: { Authorization: "Bearer token" } }),
  );
});
it.each([
  "not json",
  "null",
  '{"success":true,"data":{"nodes":[],"edges":[],"next_after":true}}',
  '{"success":true,"data":{"nodes":[{"id":3}],"edges":[]}}',
  '{"success":true,"data":{"nodes":[],"edges":[{}],"next_after":null}}',
  '{"success":true,"data":{}}',
])("rejects malformed graph %s", async (body) => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(body)));
  await expect(
    new ApiClient("token").graph(new AbortController().signal),
  ).rejects.toThrow();
});
it("rejects malformed query response", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(
        new Response(
          '{"success":true,"data":{"evidence":{"nodes":[],"edges":[]}}}',
        ),
      ),
  );
  await expect(
    new ApiClient("token").query("Rust", new AbortController().signal),
  ).rejects.toThrow();
});
it.each([
  { answer: {} },
  {
    evidence: {
      nodes: [{ id: "1", data: "bad", score: 1, hop: 1, confidence: 1 }],
      edges: [],
    },
  },
  {
    citations: [{ node_id: "1", source: "s", confidence: 1, span: "invalid" }],
  },
  { groundedness: "invalid" },
  {
    evidence: {
      nodes: [],
      edges: [{ source: "1", target: "2", relation: "mentions" }],
    },
  },
])("validates all rendered query fields %j", async (patch) => {
  const data = {
    answer: "text",
    evidence: { nodes: [], edges: [] },
    citations: [],
    model_id: null,
    snapshot_id: null,
    groundedness: 1,
    latency_ms: 1,
    ...patch,
  };
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify({ success: true, data }))),
  );
  const result = new ApiClient("token").query(
    "test",
    new AbortController().signal,
  );
  if ("evidence" in patch && patch.evidence?.edges.length)
    expect((await result).evidence.edges[0].source).toBe("1");
  else await expect(result).rejects.toThrow();
});
it("rejects oversized and absent response streams", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(new Response("x".repeat(8 * 1024 * 1024 + 1))),
  );
  await expect(
    new ApiClient("token").graph(new AbortController().signal),
  ).rejects.toThrow("size limit");
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null)));
  await expect(
    new ApiClient("token").graph(new AbortController().signal),
  ).rejects.toThrow();
});
