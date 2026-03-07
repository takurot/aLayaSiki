import React from 'react';
import type { Node } from '../types';
import { Info, Tag, Network, MapPin, Database, Cpu } from 'lucide-react';

interface DetailsPanelProps {
  selectedNode: Node | null;
  onClose: () => void;
}

const DetailsPanel: React.FC<DetailsPanelProps> = ({ selectedNode, onClose }) => {
  if (!selectedNode) {
    return (
      <div className="h-full flex items-center justify-center p-6 text-slate-500 bg-white border-l border-slate-200">
        <div className="text-center space-y-3">
          <Info className="mx-auto h-8 w-8 opacity-50" />
          <p>Select a node or edge in the graph to view details.</p>
        </div>
      </div>
    );
  }

  const parseMetadata = (metadata?: string) => {
    if (!metadata) return null;
    try {
      return JSON.parse(metadata);
    } catch {
      return { raw: metadata };
    }
  };

  const parsedMetadata = parseMetadata(selectedNode.metadata);

  return (
    <div className="h-full flex flex-col bg-white border-l border-slate-200 overflow-y-auto">
      <div className="flex items-center justify-between p-4 border-b border-slate-200 bg-slate-50">
        <h2 className="text-lg font-semibold text-slate-800 flex items-center gap-2">
          <Network className="h-5 w-5 text-indigo-500" />
          Node Details
        </h2>
        <button
          onClick={onClose}
          className="text-slate-400 hover:text-slate-600 p-1 rounded-md hover:bg-slate-200 transition-colors"
        >
          &times;
        </button>
      </div>

      <div className="p-5 space-y-6">
        <div>
          <h3 className="text-xl font-bold text-slate-900 mb-1">{selectedNode.label}</h3>
          <div className="flex items-center gap-2 text-sm text-slate-500 font-mono">
            <Tag className="h-4 w-4" />
            <span>ID: {selectedNode.id}</span>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div className="bg-slate-50 p-3 rounded-lg border border-slate-100">
            <div className="text-xs text-slate-500 uppercase font-semibold mb-1 flex items-center gap-1">
              <MapPin className="h-3 w-3" /> Community
            </div>
            <div className="font-medium">{selectedNode.community !== undefined ? selectedNode.community : 'N/A'}</div>
          </div>
          <div className="bg-slate-50 p-3 rounded-lg border border-slate-100">
            <div className="text-xs text-slate-500 uppercase font-semibold mb-1 flex items-center gap-1">
              <Cpu className="h-3 w-3" /> Confidence
            </div>
            <div className="font-medium">
              {selectedNode.confidence !== undefined
                ? `${(selectedNode.confidence * 100).toFixed(1)}%`
                : 'N/A'}
            </div>
          </div>
        </div>

        {selectedNode.model_id && (
          <div className="space-y-2">
            <h4 className="text-sm font-semibold text-slate-700 uppercase tracking-wider flex items-center gap-2">
              <Database className="h-4 w-4" /> Extraction Model
            </h4>
            <div className="bg-indigo-50 text-indigo-700 text-sm py-2 px-3 rounded-md font-mono">
              {selectedNode.model_id}
            </div>
          </div>
        )}

        {selectedNode.provenance && (
          <div className="space-y-2">
            <h4 className="text-sm font-semibold text-slate-700 uppercase tracking-wider">Provenance</h4>
            <div className="text-sm text-slate-600 bg-slate-50 p-3 rounded-md border border-slate-200">
              {selectedNode.provenance}
            </div>
          </div>
        )}

        {parsedMetadata && (
          <div className="space-y-2">
            <h4 className="text-sm font-semibold text-slate-700 uppercase tracking-wider">Metadata</h4>
            <div className="bg-slate-900 text-slate-200 p-3 rounded-md text-sm font-mono overflow-x-auto">
              <pre>{JSON.stringify(parsedMetadata, null, 2)}</pre>
            </div>
          </div>
        )}

        {selectedNode.embedding && (
          <div className="space-y-2">
            <h4 className="text-sm font-semibold text-slate-700 uppercase tracking-wider">Embedding Vector</h4>
            <div className="text-xs text-slate-500 bg-slate-50 p-2 rounded break-all font-mono">
              [{selectedNode.embedding.slice(0, 5).join(', ')} ... ({selectedNode.embedding.length} dims)]
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default DetailsPanel;
