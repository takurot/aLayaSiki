// Placeholder response generator used until the chat is wired to a real backend.
export interface MockChatResponse {
  text: string;
  highlightIds: number[];
}

export function getMockChatResponse(query: string): MockChatResponse {
  const lowerQuery = query.toLowerCase();

  if (lowerQuery.includes('apple') || lowerQuery.includes('vision pro')) {
    return {
      text: `I found some relevant information regarding "${query}". Here are the key entities connected to Apple and Vision Pro.`,
      highlightIds: [1, 2, 4], // Mock IDs matching the sample data
    };
  }

  if (lowerQuery.includes('meta') || lowerQuery.includes('quest')) {
    return {
      text: `I found some relevant information regarding "${query}". Here is the cluster related to Meta and VR/AR competition.`,
      highlightIds: [3, 4],
    };
  }

  return {
    text: 'I processed your query. Let me highlight some key nodes across the graph that might be relevant.',
    highlightIds: [1, 5, 8],
  };
}
