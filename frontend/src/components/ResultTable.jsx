const LABELS = {
  list_projects: 'Projects',
  list_subscriptions: 'Subscriptions',
  list_schedules: 'Schedules',
  list_cube_caches: 'Cube caches',
  search_objects: 'Search results',
}

function fmt(value) {
  if (value === true) return 'yes'
  if (value === false) return 'no'
  if (Array.isArray(value)) return value.join(', ')
  if (value === null || value === undefined) return ''
  return String(value)
}

export default function ResultTable({ tool, data }) {
  if (Array.isArray(data)) {
    if (data.length === 0) return <div className="table-empty">No results.</div>
    const columns = [...new Set(data.flatMap((row) => Object.keys(row)))].filter(
      (c) => !c.endsWith('_id') && c !== 'id',
    )
    return (
      <div className="table-wrap">
        <div className="table-title">{LABELS[tool] || tool}</div>
        <table>
          <thead>
            <tr>{columns.map((c) => <th key={c}>{c.replace(/_/g, ' ')}</th>)}</tr>
          </thead>
          <tbody>
            {data.map((row, i) => (
              <tr key={i}>
                {columns.map((c) => <td key={c}>{fmt(row[c])}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }
  if (data && typeof data === 'object') {
    const entries = Object.entries(data).filter(([k]) => !k.endsWith('_id') && k !== 'id')
    return (
      <div className="table-wrap">
        <table>
          <tbody>
            {entries.map(([k, v]) => (
              <tr key={k}>
                <th className="kv">{k.replace(/_/g, ' ')}</th>
                <td>{fmt(v)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }
  return null
}
