import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { it, expect, vi, beforeEach } from "vitest";
import App from "./App";
const page = {
  success: true,
  data: {
    nodes: [
      {
        id: "18446744073709551614",
        data: "Rust company",
        metadata: { source: "fixture.md" },
      },
    ],
    edges: [],
    next_after: null,
  },
};
const answer = {
  success: true,
  data: {
    answer: "A real answer",
    evidence: {
      nodes: [
        {
          id: "18446744073709551614",
          data: "Rust company",
          score: 0.8,
          hop: 1,
          confidence: 0.9,
          provenance: { source: "fixture.md" },
        },
      ],
      edges: [],
    },
    citations: [
      {
        node_id: "18446744073709551614",
        source: "fixture.md",
        span: [0, 4],
        confidence: 0.9,
      },
    ],
    model_id: "model1",
    snapshot_id: "wal-lsn-2",
    groundedness: 0.8,
    latency_ms: 1,
  },
};
beforeEach(() => {
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});
function connect() {
  fireEvent.change(screen.getByLabelText("Access token"), {
    target: { value: "token" },
  });
  fireEvent.click(screen.getByText("Connect"));
}
it("connects graph, renders evidence, clears data on disconnect", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(page)))
      .mockResolvedValueOnce(new Response(JSON.stringify(answer))),
  );
  render(<App />);
  expect(screen.getByLabelText("Query")).toBeDisabled();
  connect();
  const node = await screen.findByRole("button", { name: "Node Rust company" });
  fireEvent.keyDown(node, { key: "Enter" });
  expect(screen.getByText("ID: 18446744073709551614")).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("Query"), {
    target: { value: "Rust strategy" },
  });
  fireEvent.click(screen.getByText("Send"));
  expect(await screen.findByText("A real answer")).toBeInTheDocument();
  expect(screen.getByText(/Model: model1/)).toBeInTheDocument();
  fireEvent.click(
    screen.getByRole("button", { name: /Rust company · fixture.md/ }),
  );
  fireEvent.click(screen.getByText("×"));
  fireEvent.click(screen.getByText("Disconnect"));
  expect(screen.queryByText("A real answer")).not.toBeInTheDocument();
  expect(
    screen.queryByRole("button", { name: "Node Rust company" }),
  ).not.toBeInTheDocument();
  expect(screen.getByLabelText("Access token")).toHaveValue("");
});
it("handles empty graph, partial graph, refresh and errors", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            ...page,
            data: {
              ...page.data,
              nodes: Array.from({ length: 1000 }, (_, index) => ({
                ...page.data.nodes[0],
                id: String(index),
              })),
              next_after: "42",
            },
          }),
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            success: true,
            data: { nodes: [], edges: [], next_after: null },
          }),
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            success: false,
            error: { message: "Access denied" },
          }),
          { status: 403 },
        ),
      ),
  );
  render(<App />);
  connect();
  expect(await screen.findByText(/Showing the first/)).toBeInTheDocument();
  fireEvent.click(screen.getByText("Refresh graph"));
  expect(await screen.findByText(/No graph data yet/)).toBeInTheDocument();
  expect(
    screen.queryByRole("button", { name: "Node Rust company" }),
  ).not.toBeInTheDocument();
  fireEvent.click(screen.getByText("Refresh graph"));
  expect(await screen.findByRole("alert")).toHaveTextContent("Access denied");
});
it("aborts stale graph and chat requests on identity switch", async () => {
  let resolveGraph: (response: Response) => void = () => {};
  const calls = vi
    .fn()
    .mockImplementationOnce(
      () =>
        new Promise<Response>((resolve) => {
          resolveGraph = resolve;
        }),
    )
    .mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          success: true,
          data: { nodes: [], edges: [], next_after: null },
        }),
      ),
    );
  vi.stubGlobal("fetch", calls);
  render(<App />);
  connect();
  expect(screen.getByText("Loading graph…")).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("Access token"), {
    target: { value: "tenant2" },
  });
  fireEvent.click(screen.getByText("Connect"));
  expect(await screen.findByText(/No graph data yet/)).toBeInTheDocument();
  resolveGraph(new Response(JSON.stringify(page)));
  await waitFor(() =>
    expect(
      screen.queryByRole("button", { name: "Node Rust company" }),
    ).not.toBeInTheDocument(),
  );
});
it("reports query errors and supports evidence-only response", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(page)))
      .mockRejectedValueOnce(new Error("Query unavailable"))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            ...answer,
            data: {
              ...answer.data,
              answer: null,
              model_id: null,
              snapshot_id: null,
              evidence: {
                nodes: [{ ...answer.data.evidence.nodes[0], provenance: {} }],
                edges: [],
              },
            },
          }),
        ),
      ),
  );
  render(<App />);
  connect();
  await screen.findByRole("button", { name: "Node Rust company" });
  fireEvent.change(screen.getByLabelText("Query"), {
    target: { value: "Rust" },
  });
  fireEvent.click(screen.getByText("Send"));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "Query unavailable",
  );
  fireEvent.change(screen.getByLabelText("Query"), {
    target: { value: "More" },
  });
  fireEvent.click(screen.getByText("Send"));
  expect(await screen.findByText("Evidence retrieved")).toBeInTheDocument();
  expect(screen.getByText(/unknown source/)).toBeInTheDocument();
});
