import Link from "next/link";

export default function NotFound() {
  return (
    <div className="page-status">
      <h2>Page not found</h2>
      <p>The page you requested does not exist or may have moved.</p>
      <div className="button-row page-status-actions">
        <Link className="button-link" href="/">
          Back to overview
        </Link>
        <Link className="button-link ghost" href="/market">
          Market Intelligence
        </Link>
      </div>
    </div>
  );
}
