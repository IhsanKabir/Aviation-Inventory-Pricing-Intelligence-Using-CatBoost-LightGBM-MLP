import { ReactNode } from "react";

export function MetricCard({
  label,
  value,
  footnote
}: {
  label: string;
  value: ReactNode;
  footnote?: ReactNode;
}) {
  return (
    <div className="card metric-card">
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
      {footnote ? <div className="metric-footnote">{footnote}</div> : null}
    </div>
  );
}
