import { parse, isLosslessNumber } from "lossless-json";
import type { GraphData, Node } from "./types";
export interface Provenance {
  source?: string;
  extraction_model_id?: string;
  snapshot_id?: string;
}
export interface EvidenceNode {
  id: string;
  data: string;
  score: number;
  hop: number;
  confidence: number;
  provenance: Provenance;
}
export interface QueryResult {
  answer: string | null;
  evidence: {
    nodes: EvidenceNode[];
    edges: { source: string; target: string; relation: string }[];
  };
  citations: {
    node_id: string;
    source: string;
    confidence: number;
    span: number[];
  }[];
  model_id: string | null;
  snapshot_id: string | null;
  groundedness: number;
  latency_ms: number;
}
export interface GraphPage extends GraphData {
  next_after: string | null;
}
function normalize(value: unknown, key = ""): unknown {
  if (isLosslessNumber(value)) {
    if (
      [
        "id",
        "source",
        "target",
        "node_id",
        "anchor_id",
        "target_id",
        "next_after",
      ].includes(key)
    )
      return value.toString();
    const number = Number(value.toString());
    if (!Number.isFinite(number)) throw new Error("Invalid response number");
    return number;
  }
  if (Array.isArray(value)) return value.map((item) => normalize(item, key));
  if (value !== null && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([name, item]) => [
        name,
        normalize(item, name),
      ]),
    );
  return value;
}
function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid API response");
  return value as Record<string, unknown>;
}
const string = (value: unknown): string => {
  if (typeof value !== "string") throw new Error("Invalid query response");
  return value;
};
const number = (value: unknown): number => {
  if (typeof value !== "number" || !Number.isFinite(value))
    throw new Error("Invalid query response");
  return value;
};
const optionalString = (value: unknown): string | undefined =>
  value === undefined ? undefined : string(value);
const nullableString = (value: unknown): string | null =>
  value === null ? null : string(value);
function decodeEvidenceNode(raw: unknown): EvidenceNode {
  const node = record(raw);
  const provenance = record(node.provenance);
  return {
    id: string(node.id),
    data: string(node.data),
    score: number(node.score),
    hop: number(node.hop),
    confidence: number(node.confidence),
    provenance: {
      source: optionalString(provenance.source),
      extraction_model_id: optionalString(provenance.extraction_model_id),
      snapshot_id: optionalString(provenance.snapshot_id),
    },
  };
}

function decodeCitation(raw: unknown): QueryResult["citations"][number] {
  const citation = record(raw);
  if (!Array.isArray(citation.span) || citation.span.length !== 2)
    throw new Error("Invalid citation span");
  return {
    node_id: string(citation.node_id),
    source: string(citation.source),
    confidence: number(citation.confidence),
    span: citation.span.map(number),
  };
}

export class ApiClient {
  private readonly token: string;
  constructor(token: string) {
    this.token = token;
  }
  private async request(
    path: string,
    signal: AbortSignal,
    body?: unknown,
  ): Promise<unknown> {
    const response = await fetch(path, {
      method: body === undefined ? "GET" : "POST",
      headers: {
        Authorization: `Bearer ${this.token}`,
        ...(body === undefined ? {} : { "Content-Type": "application/json" }),
      },
      signal,
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const reader = response.body?.getReader();
    if (!reader) throw new Error("Invalid API response");
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        size += chunk.value.byteLength;
        if (size > 8 * 1024 * 1024) {
          await reader.cancel();
          throw new Error("Response exceeds size limit");
        }
        chunks.push(chunk.value);
      }
    } finally {
      reader.releaseLock();
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    const text = new TextDecoder().decode(bytes);
    let envelope: Record<string, unknown>;
    try {
      envelope = record(normalize(parse(text)));
    } catch {
      throw new Error("Invalid API response");
    }
    if (!response.ok || envelope.success !== true) {
      const error = record(envelope.error);
      throw new Error(
        typeof error.message === "string" ? error.message : "Request failed",
      );
    }
    return envelope.data;
  }
  async graph(signal: AbortSignal): Promise<GraphPage> {
    const data = record(await this.request("/v1/graph?limit=1000", signal));
    if (!Array.isArray(data.nodes) || !Array.isArray(data.edges))
      throw new Error("Invalid graph response");
    const nodes = data.nodes.map((raw) => {
      const node = record(raw);
      const metadata = record(node.metadata ?? {});
      if (typeof node.id !== "string" || typeof node.data !== "string")
        throw new Error("Invalid graph node");
      return {
        id: node.id,
        label: node.data,
        metadata,
        provenance:
          typeof metadata.source === "string" ? metadata.source : undefined,
      } satisfies Node;
    });
    const edges = data.edges.map((raw) => {
      const edge = record(raw);
      if (
        typeof edge.source !== "string" ||
        typeof edge.target !== "string" ||
        typeof edge.relation !== "string"
      )
        throw new Error("Invalid graph edge");
      return {
        source: edge.source,
        target: edge.target,
        relation_type: edge.relation,
      };
    });
    if (data.next_after !== null && typeof data.next_after !== "string")
      throw new Error("Invalid graph cursor");
    return { nodes, edges, next_after: data.next_after };
  }
  async query(query: string, signal: AbortSignal): Promise<QueryResult> {
    const data = record(
      await this.request("/v1/query", signal, {
        query,
        mode: "answer",
        search_mode: "local",
        top_k: 20,
        traversal: { depth: 2 },
      }),
    );
    const evidence = record(data.evidence);
    if (
      !Array.isArray(evidence.nodes) ||
      !Array.isArray(evidence.edges) ||
      !Array.isArray(data.citations)
    )
      throw new Error("Invalid query response");
    const nodes = evidence.nodes.map(decodeEvidenceNode);
    const edges = evidence.edges.map((raw) => {
      const edge = record(raw);
      return {
        source: string(edge.source),
        target: string(edge.target),
        relation: string(edge.relation),
      };
    });
    const citations = data.citations.map(decodeCitation);
    return {
      answer: nullableString(data.answer),
      evidence: { nodes, edges },
      citations,
      model_id: nullableString(data.model_id),
      snapshot_id: nullableString(data.snapshot_id),
      groundedness: number(data.groundedness),
      latency_ms: number(data.latency_ms),
    };
  }
}
