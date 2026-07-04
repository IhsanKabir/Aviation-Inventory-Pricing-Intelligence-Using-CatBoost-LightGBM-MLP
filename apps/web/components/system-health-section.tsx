import { MetricCard } from "@/components/metric-card";
import { DataPanel } from "@/components/data-panel";
import { formatDhakaDateTime } from "@/lib/format";
import type { SystemHealth } from "@/lib/system-health";

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

function ServicePill({ up, label }: { up: boolean; label: string }) {
  return (
    <div className="sh-service">
      <span className={`sh-dot ${up ? "sh-dot--up" : "sh-dot--down"}`} aria-hidden />
      <strong>{label}</strong>
      <span className={`pill ${up ? "good" : "warn"}`}>{up ? "Up" : "Down"}</span>
    </div>
  );
}

export function SystemHealthSection({
  health,
  error,
}: {
  health: SystemHealth | null;
  error?: string;
}) {
  if (!health) {
    return (
      <DataPanel title="System health" copy="Live service status, error rate, and recent errors.">
        <div className="status-banner warn">
          System health is unavailable: {error ?? "no data"}. The API may be down — which is itself
          the signal.
        </div>
      </DataPanel>
    );
  }

  const req = health.requests_1h;
  const errorRatePct = (req.error_rate * 100).toFixed(2);
  const allUp = health.services.api && health.services.database && health.services.bigquery;

  return (
    <section className="sh-block">
      <div className="sh-header">
        <h2 className="page-title" style={{ marginBottom: 4 }}>System Health</h2>
        <span className={`pill ${allUp && health.errors_24h === 0 ? "good" : health.errors_24h > 0 ? "warn" : "good"}`}>
          {allUp ? (health.errors_24h > 0 ? "Degraded — errors present" : "All systems operational") : "Service down"}
        </span>
      </div>

      <div className="sh-services">
        <ServicePill up={health.services.api} label="API" />
        <ServicePill up={health.services.database} label="Database" />
        <ServicePill up={health.services.bigquery} label="BigQuery" />
      </div>

      <div className="grid cards">
        <MetricCard
          label="Errors (24h)"
          value={String(health.errors_24h)}
          footnote={health.errors_24h === 0 ? "No errors recorded" : "Recorded server errors"}
        />
        <MetricCard
          label="Error rate (1h)"
          value={`${errorRatePct}%`}
          footnote={`${req.error_requests}/${req.total_requests} requests · this instance`}
        />
        <MetricCard
          label="Latency p50 / p95"
          value={`${req.latency_p50_ms} / ${req.latency_p95_ms} ms`}
          footnote="Rolling 1h · this instance"
        />
        <MetricCard
          label="Version · uptime"
          value={`${health.version}`}
          footnote={`up ${formatUptime(health.uptime_seconds)} · inst ${health.instance_id}`}
        />
      </div>

      <div className="section-grid">
        <DataPanel
          title="Recent errors"
          copy="Most recent server errors (5xx + unhandled). Empty is good."
        >
          {!health.recent_errors.length ? (
            <div className="empty-state">No recent errors. 🎉</div>
          ) : (
            <div className="table-list">
              {health.recent_errors.slice(0, 15).map((err, i) => (
                <div className="table-row" key={`${err.occurred_at_utc}-${i}`}>
                  <div>
                    <strong>
                      {err.method} {err.path}
                    </strong>
                    <span className="mono">{err.error_type}: {err.message?.slice(0, 140)}</span>
                  </div>
                  <div className="pill warn">{err.status ?? "-"}</div>
                  <span>{err.occurred_at_utc ? formatDhakaDateTime(err.occurred_at_utc) : "-"}</span>
                </div>
              ))}
            </div>
          )}
        </DataPanel>

        <DataPanel
          title="Data freshness"
          copy="Latest artifacts flowing through the platform."
        >
          <div className="table-list">
            <div className="table-row">
              <div>
                <strong>Discount report</strong>
                <span>Most recent synced OTA comparison</span>
              </div>
              <div className={`pill ${health.latest_discount_report_date ? "good" : "warn"}`}>
                {health.latest_discount_report_date ?? "none yet"}
              </div>
              <span>—</span>
            </div>
          </div>
        </DataPanel>
      </div>
    </section>
  );
}
