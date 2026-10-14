import { render, fireEvent, screen, act } from "@testing-library/react";
import { it, expect, vi } from "vitest";
import GraphExplorer from "./GraphExplorer";
import DetailsPanel from "./DetailsPanel";
it("draws links and hulls, resizes, highlights and supports pointer/keyboard", async () => {
  let resize = () => {};
  vi.stubGlobal(
    "ResizeObserver",
    class {
      constructor(callback: () => void) {
        resize = callback;
      }
      observe() {}
      disconnect() {}
    },
  );
  const data = {
    nodes: [
      { id: "1", label: "One", community: 1 },
      { id: "2", label: "Two", community: 1 },
      { id: "3", label: "Three", community: 1 },
    ],
    edges: [
      { source: "1", target: "2", relation_type: "mentions" },
      { source: "2", target: "3", relation_type: "mentions" },
    ],
  };
  const click = vi.fn();
  const { rerender } = render(
    <GraphExplorer
      data={data}
      onNodeClick={click}
      selectedNodeId="1"
      highlightedNodeId="2"
    />,
  );
  const one = screen.getByRole("button", { name: "Node One" });
  fireEvent.click(one);
  fireEvent.keyDown(one, { key: " " });
  fireEvent.keyDown(one, { key: "Escape" });
  expect(click).toHaveBeenCalledTimes(2);
  function mouse(target: Element | Window, type: string, x: number) {
    const event = new MouseEvent(type, {
      bubbles: true,
      clientX: x,
      clientY: x,
    });
    Object.defineProperty(event, "view", { value: window });
    fireEvent(target, event);
  }
  mouse(one, "mousedown", 10);
  mouse(window, "mousemove", 20);
  mouse(window, "mouseup", 20);
  act(() => resize());
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 140));
  });
  expect(document.querySelector("path")).toBeInTheDocument();
  rerender(
    <GraphExplorer data={{ nodes: [], edges: [] }} onNodeClick={click} />,
  );
  expect(
    screen.queryByRole("button", { name: "Node One" }),
  ).not.toBeInTheDocument();
});
it("displays all available details and missing values", () => {
  const { rerender } = render(
    <DetailsPanel
      selectedNode={{
        id: "42",
        label: "Detailed",
        community: 1,
        confidence: 0.9,
        model_id: "model1",
        embedding: [0.1, 0.2],
        metadata: { type: "company" },
        provenance: "test.md",
      }}
      onClose={() => {}}
    />,
  );
  expect(screen.getByText("model1")).toBeInTheDocument();
  rerender(
    <DetailsPanel
      selectedNode={{ id: "43", label: "Minimal" }}
      onClose={() => {}}
    />,
  );
  expect(screen.getAllByText("N/A").length).toBeGreaterThan(0);
});
