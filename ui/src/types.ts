export interface Node {
  id: number;
  label: string;
  community?: number;
  group?: number;
  embedding?: number[];
  metadata?: string;
  provenance?: string;
  confidence?: number;
  model_id?: string;
}

export interface Edge {
  source: number;
  target: number;
  relation_type: number;
  weight?: number;
  direction?: number;
  provenance?: string;
  confidence?: number;
}

export interface GraphData {
  nodes: Node[];
  edges: Edge[];
}
