export type RelationType = string;

export type EdgeDirection = "directed" | "undirected";

export interface Node {
  id: string;
  label: string;
  community?: number;
  embedding?: number[];
  metadata?: Record<string, unknown>;
  provenance?: string;
  confidence?: number;
  model_id?: string;
}

export interface Edge {
  source: string;
  target: string;
  relation_type: RelationType;
  weight?: number;
  direction?: EdgeDirection;
  provenance?: string;
  confidence?: number;
}

export interface GraphData {
  nodes: Node[];
  edges: Edge[];
}
