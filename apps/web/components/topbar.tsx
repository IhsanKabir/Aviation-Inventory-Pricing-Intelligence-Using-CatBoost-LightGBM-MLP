import Link from "next/link";

const NAV_ITEMS = [
  { href: "/", label: "Overview" },
  { href: "/routes", label: "Routes" },
  { href: "/changes", label: "Changes" },
  { href: "/forecasting", label: "Forecasting" }
];

export function Topbar({ pathname }: { pathname: string }) {
  return (
    <div className="topbar">
      <div className="shell topbar-inner">
        <div className="brand">
          <div className="brand-mark">AP</div>
          <div>
            <div className="brand-title">Aero Pulse Intelligence Monitor</div>
            <div className="brand-subtitle">
              FastAPI + Next.js operational shell
            </div>
          </div>
        </div>
        <nav className="nav" aria-label="Primary">
          {NAV_ITEMS.map((item) => (
            <Link
              key={item.href}
              className="nav-link"
              href={item.href}
              data-active={pathname === item.href}
            >
              {item.label}
            </Link>
          ))}
        </nav>
      </div>
    </div>
  );
}
