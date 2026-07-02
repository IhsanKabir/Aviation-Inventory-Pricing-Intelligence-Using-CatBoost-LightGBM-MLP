export default function Loading() {
  return (
    <div className="page-status" role="status">
      <div className="page-status-spinner" aria-hidden="true" />
      <h2>Loading the latest data…</h2>
      <p>Fetching the most recent published snapshot for this view.</p>
    </div>
  );
}
