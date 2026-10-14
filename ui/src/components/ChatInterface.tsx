import { useState, useRef, useEffect } from "react";
import type { ApiClient, QueryResult } from "../api";
interface Message {
  id: string;
  text: string;
  response?: QueryResult;
  error?: boolean;
}
interface Props {
  client: ApiClient | null;
  onHighlightNodes: (ids: string[], response: QueryResult) => void;
}
export default function ChatInterface({ client, onHighlightNodes }: Props) {
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [pending, setPending] = useState(false);
  const controller = useRef<AbortController | null>(null);
  useEffect(() => () => controller.current?.abort(), []);
  async function submit(event: React.FormEvent) {
    event.preventDefault();
    if (!client || pending || !input.trim()) return;
    const query = input.trim();
    setInput("");
    setPending(true);
    setMessages((previous) => [
      ...previous,
      { id: crypto.randomUUID(), text: query },
    ]);
    const active = new AbortController();
    controller.current = active;
    try {
      const response = await client.query(query, active.signal);
      if (active.signal.aborted) return;
      setMessages((previous) => [
        ...previous,
        {
          id: crypto.randomUUID(),
          text: response.answer ?? "Evidence retrieved",
          response,
        },
      ]);
    } catch (error) {
      if (!active.signal.aborted)
        setMessages((previous) => [
          ...previous,
          {
            id: crypto.randomUUID(),
            text: error instanceof Error ? error.message : "Query failed",
            error: true,
          },
        ]);
    } finally {
      if (!active.signal.aborted) setPending(false);
    }
  }
  return (
    <section
      className="flex flex-col h-full bg-white border-r border-slate-200"
      aria-label="Chat"
    >
      <h2 className="p-4 font-semibold">aLayaSiki Copilot</h2>
      <div className="flex-1 overflow-y-auto p-4 space-y-4" aria-live="polite">
        {messages.map((message) => (
          <article key={message.id} className="border rounded p-3">
            <p role={message.error ? "alert" : undefined}>{message.text}</p>
            {message.response && (
              <>
                <p className="text-xs">
                  Model: {message.response.model_id ?? "unspecified"} ·
                  Snapshot: {message.response.snapshot_id ?? "unspecified"}
                </p>
                <p className="text-xs">
                  Groundedness:{" "}
                  {(message.response.groundedness * 100).toFixed(1)}%
                </p>
                <h3>Evidence</h3>
                {message.response.evidence.nodes.map((node) => (
                  <button
                    className="block text-left text-sm text-indigo-700"
                    key={node.id}
                    onClick={() => {
                      if (message.response)
                        onHighlightNodes([node.id], message.response);
                    }}
                  >
                    {node.data} · {node.provenance.source ?? "unknown source"} ·
                    ID {node.id}
                  </button>
                ))}
                <h3>Citations</h3>
                {message.response.citations.map((citation, index) => (
                  <p className="text-xs" key={index}>
                    {citation.source} [{citation.span.join("–")}] · node{" "}
                    {citation.node_id}
                  </p>
                ))}
              </>
            )}
          </article>
        ))}
        {pending && <p role="status">Searching…</p>}
        {!client && <p>Connect to query your database.</p>}
      </div>
      <form
        className="p-4 flex gap-2"
        onSubmit={(event) => {
          void submit(event);
        }}
      >
        <input
          aria-label="Query"
          className="border rounded p-2 min-w-0 flex-1"
          value={input}
          onChange={(event) => setInput(event.target.value)}
          disabled={!client || pending}
        />
        <button disabled={!client || pending || !input.trim()}>Send</button>
      </form>
    </section>
  );
}
