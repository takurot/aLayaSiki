import React, { useEffect, useRef } from 'react';
import * as d3 from 'd3';
import type { Node, GraphData } from '../types';

interface GraphExplorerProps {
  data: GraphData;
  onNodeClick: (node: Node) => void;
  selectedNodeId?: number | null;
  highlightedNodeId?: number | null;
}

// Extend d3 simulation types to include x and y which are injected by d3
interface SimulationNode extends Node, d3.SimulationNodeDatum {}
interface SimulationLink extends d3.SimulationLinkDatum<SimulationNode> {
  relation_type: number;
}

const GraphExplorer: React.FC<GraphExplorerProps> = ({
  data,
  onNodeClick,
  selectedNodeId,
  highlightedNodeId
}) => {
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!svgRef.current || !containerRef.current || !data || data.nodes.length === 0) return;

    const width = containerRef.current.clientWidth;
    const height = containerRef.current.clientHeight;

    // Clear previous graph entirely before re-rendering
    d3.select(svgRef.current).selectAll('*').remove();

    // Map the node array to object references so links can map to them
    const nodes: SimulationNode[] = data.nodes.map(d => ({ ...d }));

    // We map edges from node IDs to actual node object references
    // This is required for d3's forceLink logic.
    const links: SimulationLink[] = data.edges.map(d => ({
      source: d.source,
      target: d.target,
      relation_type: d.relation_type,
    }));

    const svg = d3.select(svgRef.current)
      .attr('width', width)
      .attr('height', height)
      .attr('viewBox', [0, 0, width, height]);

    const g = svg.append('g');

    // Add zoom capabilities
    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 4])
      .on('zoom', (event) => {
        g.attr('transform', event.transform);
      });

    svg.call(zoom as any);

    // Color scale for communities/groups
    const colorScale = d3.scaleOrdinal(d3.schemeCategory10);

    const simulation = d3.forceSimulation<SimulationNode>(nodes)
      .force('link', d3.forceLink<SimulationNode, SimulationLink>(links).id(d => d.id).distance(150))
      .force('charge', d3.forceManyBody().strength(-400))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collide', d3.forceCollide().radius(40));

    // Draw links
    const link = g.append('g')
      .attr('stroke', '#999')
      .attr('stroke-opacity', 0.6)
      .selectAll('line')
      .data(links)
      .join('line')
      .attr('stroke-width', 2);

    // Define drag behavior
    const drag = (simulation: d3.Simulation<SimulationNode, undefined>) => {
      function dragstarted(event: any) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        event.subject.fx = event.subject.x;
        event.subject.fy = event.subject.y;
      }

      function dragged(event: any) {
        event.subject.fx = event.x;
        event.subject.fy = event.y;
      }

      function dragended(event: any) {
        if (!event.active) simulation.alphaTarget(0);
        event.subject.fx = null;
        event.subject.fy = null;
      }

      return d3.drag<any, SimulationNode>()
        .on('start', dragstarted)
        .on('drag', dragged)
        .on('end', dragended);
    };

    // Draw community groupings (convex hulls) - basic visual clustering
    const communityGroups = Array.from(d3.group(nodes, d => d.community || d.group || 0));

    // We only draw hulls for communities with > 2 nodes
    const validCommunities = communityGroups.filter(c => c[1].length > 2);

    const hullPath = g.append('g')
      .selectAll('path')
      .data(validCommunities)
      .join('path')
      .attr('fill', d => colorScale(String(d[0])))
      .attr('fill-opacity', 0.1)
      .attr('stroke', d => colorScale(String(d[0])))
      .attr('stroke-width', 2)
      .attr('stroke-opacity', 0.5)
      .style('pointer-events', 'none');

    // Draw nodes
    const node = g.append('g')
      .attr('stroke', '#fff')
      .attr('stroke-width', 1.5)
      .selectAll('circle')
      .data(nodes)
      .join('circle')
      .attr('class', 'graph-node')
      .attr('id', d => `node-${d.id}`)
      .attr('r', 20)
      .attr('fill', d => colorScale(String(d.community || d.group || 0)))
      .call(drag(simulation))
      .on('click', (_event, d) => {
        onNodeClick(d as Node);
      });

    // Add labels
    const label = g.append('g')
      .selectAll('text')
      .data(nodes)
      .join('text')
      .attr('dx', 24)
      .attr('dy', '.35em')
      .text(d => d.label)
      .style('font-size', '14px')
      .style('font-family', 'sans-serif')
      .style('pointer-events', 'none');

    // Add titles (tooltips)
    node.append('title')
      .text(d => `${d.label} (ID: ${d.id})\nCommunity: ${d.community || 'N/A'}`);

    // Tick function to update positions
    simulation.on('tick', () => {
      link
        .attr('x1', d => (d.source as SimulationNode).x!)
        .attr('y1', d => (d.source as SimulationNode).y!)
        .attr('x2', d => (d.target as SimulationNode).x!)
        .attr('y2', d => (d.target as SimulationNode).y!);

      node
        .attr('cx', d => d.x!)
        .attr('cy', d => d.y!);

      label
        .attr('x', d => d.x!)
        .attr('y', d => d.y!);

      // Update community hulls
      hullPath.attr('d', d => {
        const points: [number, number][] = d[1].map(n => [n.x || 0, n.y || 0]);
        if (points.length < 3) return null;
        // Add padding to hull
        const hull = d3.polygonHull(points);
        if (!hull) return null;
        return `M${hull.join('L')}Z`;
      });
    });

    const handleResize = () => {
      if (!containerRef.current || !svgRef.current) return;
      const newWidth = containerRef.current.clientWidth;
      const newHeight = containerRef.current.clientHeight;
      d3.select(svgRef.current).attr('width', newWidth).attr('height', newHeight).attr('viewBox', [0, 0, newWidth, newHeight]);
      simulation.force('center', d3.forceCenter(newWidth / 2, newHeight / 2));
      simulation.alpha(0.3).restart();
    };

    window.addEventListener('resize', handleResize);

    return () => {
      simulation.stop();
      window.removeEventListener('resize', handleResize);
    };
  }, [data]);

  // Handle highlights externally without restarting simulation
  useEffect(() => {
    if (!svgRef.current) return;
    const svg = d3.select(svgRef.current);

    svg.selectAll('.graph-node')
      .attr('stroke', (d: any) => {
        if (d.id === selectedNodeId) return '#333';
        if (d.id === highlightedNodeId) return '#ff0000';
        return '#fff';
      })
      .attr('stroke-width', (d: any) => {
        if (d.id === selectedNodeId) return 4;
        if (d.id === highlightedNodeId) return 5;
        return 1.5;
      });
  }, [selectedNodeId, highlightedNodeId]);

  return (
    <div ref={containerRef} style={{ width: '100%', height: '100%', backgroundColor: '#f8fafc' }}>
      <svg ref={svgRef}></svg>
    </div>
  );
};

export default GraphExplorer;
