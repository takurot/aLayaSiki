export type RelationType =
  | 'produces'
  | 'leads'
  | 'is_a'
  | 'related_to'
  | 'competes_with';

export type EdgeDirection = 'directed' | 'undirected';

export interface Node {
  id: number;
  label: string;
  community?: number;
  embedding?: number[];
  metadata?: Record<string, unknown>;
  provenance?: string;
  confidence?: number;
  model_id?: string;
}

export interface Edge {
  source: number;
  target: number;
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
