const STATE_LABELS = {
  confirmed: '✓ Confirmed — executing',
  declined: '✕ Declined',
  superseded: 'Superseded by a newer message',
}

export default function ConfirmCard({ action, state, onResolve, disabled }) {
  const open = state === 'open'
  return (
    <div className={`confirm-card ${open ? '' : 'resolved'}`} data-testid="confirm-card">
      <div className="confirm-head">
        <span className="confirm-icon" aria-hidden>⚠</span>
        <span>Confirmation required — {action.tool_name.replace(/_/g, ' ')}</span>
      </div>
      <p className="confirm-preview">{action.preview}</p>
      {open ? (
        <div className="confirm-actions">
          <button
            className="btn-confirm"
            data-testid="confirm-btn"
            disabled={disabled}
            onClick={() => onResolve(action.action_id, true)}
          >
            Confirm &amp; execute
          </button>
          <button
            className="btn-cancel"
            data-testid="cancel-btn"
            disabled={disabled}
            onClick={() => onResolve(action.action_id, false)}
          >
            Cancel
          </button>
        </div>
      ) : (
        <div className="confirm-state">{STATE_LABELS[state] || state}</div>
      )}
    </div>
  )
}
