import { useState, useEffect, useMemo } from "react";
import GraphExplorer from "./components/GraphExplorer";
import DetailsPanel from "./components/DetailsPanel";
import ChatInterface from "./components/ChatInterface";
import { ApiClient, type QueryResult } from "./api";
import type { Node, GraphData } from "./types";
const EMPTY: GraphData = { nodes: [], edges: [] };
export default function App() {
  const [token, setToken] = useState("");
  const [connectedToken, setConnectedToken] = useState("");
  const [generation, setGeneration] = useState(0);
  const [graph, setGraph] = useState<GraphData>(EMPTY);
  const [selected, setSelected] = useState<Node | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [partial, setPartial] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const client = useMemo(
    () => (connectedToken ? new ApiClient(connectedToken) : null),
    [connectedToken],
  );
  useEffect(() => {
    if (!client) return;
    const controller = new AbortController();
    void client
      .graph(controller.signal)
      .then((page) => {
        if (controller.signal.aborted) return;
        setGraph(page);
        setSelected(null);
        setPartial(page.nodes.length >= 1000 && page.next_after !== null);
        setLoading(false);
      })
      .catch((cause: unknown) => {
        if (controller.signal.aborted) return;
        setError(
          cause instanceof Error ? cause.message : "Graph request failed",
        );
        setLoading(false);
      });
    return () => controller.abort();
  }, [client, refresh]);
  function reset() {
    setGraph(EMPTY);
    setSelected(null);
    setError("");
    setPartial(false);
    setGeneration((value) => value + 1);
  }
  function connect(event: React.FormEvent) {
    event.preventDefault();
    reset();
    setLoading(true);
    setConnectedToken(token.trim());
    setToken("");
    setRefresh((value) => value + 1);
  }
  function disconnect() {
    reset();
    setConnectedToken("");
    setToken("");
    setLoading(false);
  }
  function highlight(ids: string[], response: QueryResult) {
    const nodes = response.evidence.nodes.map((node) => ({
      id: node.id,
      label: node.data,
      confidence: node.confidence,
      provenance: node.provenance.source,
      model_id: node.provenance.extraction_model_id,
      metadata: { snapshot_id: node.provenance.snapshot_id },
    }));
    const idsInGraph = new Set(nodes.map((node) => node.id));
    const edges = response.evidence.edges
      .filter(
        (edge) => idsInGraph.has(edge.source) && idsInGraph.has(edge.target),
      )
      .map((edge) => ({
        source: edge.source,
        target: edge.target,
        relation_type: edge.relation,
      }));
    setGraph({ nodes, edges });
    setPartial(false);
    setSelected(nodes.find((node) => node.id === ids[0]) ?? null);
  }
  return (
    <div className="h-screen w-screen flex flex-col bg-slate-50 text-slate-900">
      <header className="bg-indigo-900 text-white p-3 flex flex-wrap gap-3 items-center">
        <h1 className="font-bold">aLayaSiki Explorer</h1>
        <form onSubmit={connect} className="flex gap-2">
          <input
            type="password"
            autoComplete="off"
            aria-label="Access token"
            placeholder="Access token"
            className="text-slate-900 p-1 rounded"
            value={token}
            onChange={(event) => setToken(event.target.value)}
          />
          <button disabled={!token.trim()}>Connect</button>
        </form>
        {client && (
          <>
            <button onClick={disconnect}>Disconnect</button>
            <button
              onClick={() => {
                setSelected(null);
                setLoading(true);
                setError("");
                setRefresh((value) => value + 1);
              }}
            >
              Refresh graph
            </button>
          </>
        )}
      </header>
      <main className="flex-1 flex overflow-hidden">
        <div className="w-80 shrink-0">
          <ChatInterface
            key={generation}
            client={client}
            onHighlightNodes={highlight}
          />
        </div>
        <section className="flex-1 relative" aria-label="Graph">
          {loading && (
            <p role="status" className="absolute z-10 p-3 bg-white">
              Loading graph…
            </p>
          )}
          {error && (
            <p role="alert" className="absolute z-10 p-3 bg-white">
              {error}
            </p>
          )}
          {!loading && !error && graph.nodes.length === 0 && (
            <p className="absolute z-10 p-3">
              {client
                ? "No graph data yet. Ingest content and refresh."
                : "Connect to explore your graph."}
            </p>
          )}
          {partial && (
            <p className="absolute z-10 bottom-0 bg-white p-3">
              Showing the first {graph.nodes.length.toLocaleString()} nodes.
              Edges are limited to this page.
            </p>
          )}
          <GraphExplorer
            data={graph}
            onNodeClick={setSelected}
            selectedNodeId={selected?.id}
            highlightedNodeId={selected?.id}
          />
        </section>
        <aside className="w-80 shrink-0">
          <DetailsPanel
            selectedNode={selected}
            onClose={() => setSelected(null)}
          />
        </aside>
      </main>
    </div>
  );
}
