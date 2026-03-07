import { ReactNode } from "react";

export function DataPanel({
  title,
  copy,
  children
}: {
  title: string;
  copy: string;
  children: ReactNode;
}) {
  return (
    <section className="card panel">
      <h2>{title}</h2>
      <div className="panel-copy">{copy}</div>
      {children}
    </section>
  );
}
