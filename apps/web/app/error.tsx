"use client";

import Link from "next/link";
import { useEffect } from "react";

export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("Page render failed", error);
  }, [error]);

  return (
    <div className="page-status" role="alert">
      <h2>Something went wrong loading this page.</h2>
      <p>
        The data service may be briefly unavailable. Your filters and place in the app are
        unchanged — retry, or head back to the overview.
      </p>
      {error.digest ? <p className="mono">Reference: {error.digest}</p> : null}
      <div className="button-row page-status-actions">
        <button className="button-link" onClick={() => reset()} type="button">
          Try again
        </button>
        <Link className="button-link ghost" href="/">
          Back to overview
        </Link>
      </div>
    </div>
  );
}
