import { useEffect, useRef, useState } from 'react'
import { sendChat, confirmAction } from './api.js'
import MessageBubble from './components/MessageBubble.jsx'
import ConfirmCard from './components/ConfirmCard.jsx'

const SUGGESTIONS = [
  'List projects',
  'Show subscriptions in Finance Analytics',
  'Pause "Daily Sales Email" in Finance Analytics',
  'What is the status of the Customer 360 cube in Sales Operations?',
]

let nextId = 1
const mid = () => nextId++

export default function App() {
  const [messages, setMessages] = useState([])
  const [conversationId, setConversationId] = useState(null)
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const bottomRef = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, busy])

  function appendAssistant(res) {
    setConversationId(res.conversation_id)
    setMessages((m) => [
      ...m,
      {
        id: mid(),
        role: 'assistant',
        kind: res.kind,
        text: res.reply,
        results: res.results || [],
        pending: res.pending_action || null,
        pendingState: res.pending_action ? 'open' : null,
      },
    ])
  }

  async function submit(text) {
    const message = (text ?? input).trim()
    if (!message || busy) return
    setInput('')
    // any open confirmation card is superseded by a new message
    setMessages((m) => [
      ...m.map((x) => (x.pendingState === 'open' ? { ...x, pendingState: 'superseded' } : x)),
      { id: mid(), role: 'user', text: message },
    ])
    setBusy(true)
    try {
      appendAssistant(await sendChat(message, conversationId))
    } catch (err) {
      setMessages((m) => [
        ...m,
        { id: mid(), role: 'assistant', kind: 'error', text: String(err.message || err) },
      ])
    } finally {
      setBusy(false)
    }
  }

  async function resolveAction(actionId, approved) {
    if (busy) return
    setBusy(true)
    setMessages((m) =>
      m.map((x) =>
        x.pending?.action_id === actionId
          ? { ...x, pendingState: approved ? 'confirmed' : 'declined' }
          : x,
      ),
    )
    try {
      appendAssistant(await confirmAction(actionId, approved))
    } catch (err) {
      setMessages((m) => [
        ...m,
        { id: mid(), role: 'assistant', kind: 'error', text: String(err.message || err) },
      ])
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="app">
      <header className="header">
        <div className="header-brand">
          <span className="logo" aria-hidden>◆</span>
          <div>
            <h1>StrategyAI</h1>
            <p>MicroStrategy Admin Copilot — subscriptions &amp; cubes</p>
          </div>
        </div>
        <span className="badge">Human-confirmed actions</span>
      </header>

      <main className="thread" data-testid="thread">
        {messages.length === 0 && (
          <div className="empty">
            <h2>What would you like to do?</h2>
            <div className="chips">
              {SUGGESTIONS.map((s) => (
                <button key={s} className="chip" onClick={() => submit(s)}>
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg) => (
          <div key={msg.id}>
            <MessageBubble msg={msg} />
            {msg.pending && (
              <ConfirmCard
                action={msg.pending}
                state={msg.pendingState}
                onResolve={resolveAction}
                disabled={busy}
              />
            )}
          </div>
        ))}

        {busy && <div className="typing" data-testid="typing">StrategyAI is working…</div>}
        <div ref={bottomRef} />
      </main>

      <footer className="composer">
        <input
          data-testid="chat-input"
          value={input}
          placeholder='e.g. pause "Daily Sales Email" in Finance Analytics'
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && submit()}
          disabled={busy}
        />
        <button data-testid="send-btn" onClick={() => submit()} disabled={busy || !input.trim()}>
          Send
        </button>
      </footer>
    </div>
  )
}
