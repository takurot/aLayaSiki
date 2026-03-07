import React, { useState, useRef, useEffect } from 'react';
import { Send, Bot, User, Sparkles, Network } from 'lucide-react';

interface Message {
  id: string;
  sender: 'user' | 'ai';
  text: string;
  highlightedNodeIds?: number[];
}

interface ChatInterfaceProps {
  onHighlightNodes: (nodeIds: number[]) => void;
}

const ChatInterface: React.FC<ChatInterfaceProps> = ({ onHighlightNodes }) => {
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState<Message[]>([
    {
      id: 'msg-0',
      sender: 'ai',
      text: 'Hello! I am your aLayaSiki assistant. Ask me anything about the data, or click a node to see its details. I can highlight entities in the graph based on your queries.',
    }
  ]);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim()) return;

    const userMsg: Message = {
      id: `msg-${Date.now()}`,
      sender: 'user',
      text: input,
    };

    setMessages(prev => [...prev, userMsg]);
    setInput('');

    // Simulate AI response and node highlighting
    setTimeout(() => {
      let responseText = `I found some relevant information regarding "${userMsg.text}".`;
      let highlightIds: number[] = [];

      // Simple mock logic based on input keywords
      const lowerInput = userMsg.text.toLowerCase();
      if (lowerInput.includes('apple') || lowerInput.includes('vision pro')) {
        responseText += ' Here are the key entities connected to Apple and Vision Pro.';
        highlightIds = [1, 2, 4]; // Mock IDs matching the sample data
      } else if (lowerInput.includes('meta') || lowerInput.includes('quest')) {
        responseText += ' Here is the cluster related to Meta and VR/AR competition.';
        highlightIds = [3, 4];
      } else {
        responseText = 'I processed your query. Let me highlight some key nodes across the graph that might be relevant.';
        // Highlight some random nodes to show interaction
        highlightIds = [1, 5, 8];
      }

      const aiMsg: Message = {
        id: `msg-${Date.now() + 1}`,
        sender: 'ai',
        text: responseText,
        highlightedNodeIds: highlightIds,
      };

      setMessages(prev => [...prev, aiMsg]);

      // Trigger the graph update
      if (highlightIds.length > 0) {
        onHighlightNodes(highlightIds);
      }
    }, 1000);
  };

  return (
    <div className="flex flex-col h-full bg-white border-r border-slate-200 shadow-sm">
      <div className="p-4 border-b border-slate-200 bg-slate-50 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-slate-800 flex items-center gap-2">
          <Sparkles className="h-5 w-5 text-indigo-500" />
          aLayaSiki Copilot
        </h2>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.map((msg) => (
          <div
            key={msg.id}
            className={`flex items-start gap-3 ${msg.sender === 'user' ? 'flex-row-reverse' : ''}`}
          >
            <div className={`flex-shrink-0 h-8 w-8 rounded-full flex items-center justify-center ${
              msg.sender === 'user' ? 'bg-indigo-600 text-white' : 'bg-emerald-500 text-white'
            }`}>
              {msg.sender === 'user' ? <User size={16} /> : <Bot size={16} />}
            </div>

            <div className={`flex flex-col max-w-[80%] ${
              msg.sender === 'user' ? 'items-end' : 'items-start'
            }`}>
              <div className={`py-2 px-3 rounded-2xl text-sm shadow-sm ${
                msg.sender === 'user'
                  ? 'bg-indigo-600 text-white rounded-tr-none'
                  : 'bg-white border border-slate-200 text-slate-800 rounded-tl-none'
              }`}>
                {msg.text}
              </div>

              {msg.highlightedNodeIds && msg.highlightedNodeIds.length > 0 && (
                <div
                  className="mt-1 flex items-center gap-1 text-xs text-indigo-600 cursor-pointer hover:underline font-medium"
                  onClick={() => onHighlightNodes(msg.highlightedNodeIds!)}
                >
                  <Network size={12} />
                  View {msg.highlightedNodeIds.length} related nodes
                </div>
              )}
            </div>
          </div>
        ))}
        <div ref={messagesEndRef} />
      </div>

      <div className="p-4 bg-white border-t border-slate-200">
        <form onSubmit={handleSubmit} className="relative flex items-center">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about the knowledge graph..."
            className="w-full pl-4 pr-12 py-3 bg-slate-50 border border-slate-300 rounded-full focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition-all shadow-sm text-sm"
          />
          <button
            type="submit"
            disabled={!input.trim()}
            className="absolute right-2 p-2 bg-indigo-600 text-white rounded-full hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            <Send size={16} />
          </button>
        </form>
      </div>
    </div>
  );
};

export default ChatInterface;
