import { useState } from 'react';
import GraphExplorer from './components/GraphExplorer';
import DetailsPanel from './components/DetailsPanel';
import ChatInterface from './components/ChatInterface';
import type { Node, GraphData } from './types';
import './App.css';
import { DatabaseZap } from 'lucide-react';

// Sample mock data resembling aLayaSiki entities
const mockData: GraphData = {
  nodes: [
    { id: 1, label: 'Apple', community: 1, embedding: [0.1, 0.2, 0.3], confidence: 0.95, provenance: 'apple_news_2024.pdf', metadata: '{"type": "Company"}' },
    { id: 2, label: 'Vision Pro', community: 1, embedding: [0.4, 0.5, 0.6], confidence: 0.88, provenance: 'apple_news_2024.pdf', metadata: '{"type": "Product", "category": "AR/VR"}' },
    { id: 3, label: 'Meta', community: 2, embedding: [0.7, 0.8, 0.9], confidence: 0.92, provenance: 'tech_report.json', metadata: '{"type": "Company"}' },
    { id: 4, label: 'Meta Quest 3', community: 2, embedding: [0.11, 0.12, 0.13], confidence: 0.99, provenance: 'tech_report.json', metadata: '{"type": "Product", "category": "VR"}' },
    { id: 5, label: 'Tim Cook', community: 1, embedding: [0.14, 0.15, 0.16], confidence: 0.91, provenance: 'apple_news_2024.pdf', metadata: '{"type": "Person", "role": "CEO"}' },
    { id: 6, label: 'Mark Zuckerberg', community: 2, embedding: [0.17, 0.18, 0.19], confidence: 0.94, provenance: 'tech_report.json', metadata: '{"type": "Person", "role": "CEO"}' },
    { id: 7, label: 'Spatial Computing', community: 3, embedding: [0.21, 0.22, 0.23], confidence: 0.85, provenance: 'industry_analysis.md', metadata: '{"type": "Concept"}' },
    { id: 8, label: 'Virtual Reality', community: 3, embedding: [0.24, 0.25, 0.26], confidence: 0.89, provenance: 'industry_analysis.md', metadata: '{"type": "Concept"}' },
    { id: 9, label: 'Mixed Reality', community: 3, embedding: [0.27, 0.28, 0.29], confidence: 0.87, provenance: 'industry_analysis.md', metadata: '{"type": "Concept"}' },
  ],
  edges: [
    { source: 1, target: 2, relation_type: 1, weight: 1.0 }, // Apple makes Vision Pro
    { source: 1, target: 5, relation_type: 2, weight: 1.0 }, // Apple CEO is Tim Cook
    { source: 3, target: 4, relation_type: 1, weight: 1.0 }, // Meta makes Quest 3
    { source: 3, target: 6, relation_type: 2, weight: 1.0 }, // Meta CEO is Mark Z
    { source: 2, target: 7, relation_type: 3, weight: 0.9 }, // Vision pro is Spatial Computing
    { source: 4, target: 8, relation_type: 3, weight: 0.9 }, // Quest is VR
    { source: 7, target: 9, relation_type: 4, weight: 0.8 }, // Spatial related to MR
    { source: 8, target: 9, relation_type: 4, weight: 0.8 }, // VR related to MR
    { source: 2, target: 4, relation_type: 5, weight: 0.7 }, // Vision Pro competes with Quest
    { source: 1, target: 3, relation_type: 5, weight: 0.8 }, // Apple competes with Meta
  ]
};

function App() {
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [highlightedNodeId, setHighlightedNodeId] = useState<number | null>(null);

  const handleNodeClick = (node: Node) => {
    setSelectedNode(node);
    setHighlightedNodeId(node.id);
  };

  const handleClosePanel = () => {
    setSelectedNode(null);
    setHighlightedNodeId(null);
  };

  const handleHighlightNodes = (nodeIds: number[]) => {
    // Just highlight the first one for simplicity in this mock,
    // or expand logic to handle multiple
    if (nodeIds.length > 0) {
      setHighlightedNodeId(nodeIds[0]);
      const node = mockData.nodes.find(n => n.id === nodeIds[0]);
      if (node) {
        setSelectedNode(node);
      }
    }
  };

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-slate-50 font-sans text-slate-900">
      {/* Header */}
      <header className="h-14 bg-indigo-900 text-white flex items-center px-6 shadow-md z-10 shrink-0">
        <div className="flex items-center gap-2">
          <DatabaseZap className="h-6 w-6 text-indigo-400" />
          <h1 className="text-xl font-bold tracking-tight">aLayaSiki Explorer</h1>
          <span className="ml-4 text-xs font-medium px-2 py-0.5 rounded-full bg-indigo-800 text-indigo-200 border border-indigo-700">
            Autonomous GraphRAG
          </span>
        </div>
      </header>

      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left: Chat Interface */}
        <div className="w-80 flex-shrink-0 relative z-10">
          <ChatInterface onHighlightNodes={handleHighlightNodes} />
        </div>

        {/* Center: Graph Explorer */}
        <div className="flex-1 relative z-0">
          <GraphExplorer
            data={mockData}
            onNodeClick={handleNodeClick}
            selectedNodeId={selectedNode?.id}
            highlightedNodeId={highlightedNodeId}
          />
        </div>

        {/* Right: Details Panel */}
        <div className="w-80 flex-shrink-0 relative z-10">
          <DetailsPanel
            selectedNode={selectedNode}
            onClose={handleClosePanel}
          />
        </div>
      </div>
    </div>
  );
}

export default App;
