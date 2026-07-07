async function post(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}))
    throw new Error(detail.detail || `Request failed (${res.status})`)
  }
  return res.json()
}

export function sendChat(message, conversationId) {
  return post('/api/chat', { message, conversation_id: conversationId })
}

export function confirmAction(actionId, approved) {
  return post(`/api/actions/${actionId}/confirm`, { approved })
}
