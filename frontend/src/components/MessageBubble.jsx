import ResultTable from './ResultTable.jsx'

export default function MessageBubble({ msg }) {
  if (msg.role === 'user') {
    return <div className="bubble user">{msg.text}</div>
  }
  return (
    <div className={`bubble assistant ${msg.kind === 'error' ? 'error' : ''}`}>
      <div className="bubble-text">{msg.text}</div>
      {(msg.results || []).map((r, i) => (
        <ResultTable key={i} tool={r.tool} data={r.data} />
      ))}
    </div>
  )
}
