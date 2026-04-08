"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { signOut as nextAuthSignOut } from "next-auth/react";

const NAV_ITEMS = [
  { href: "/", label: "Overview" },
  { href: "/routes", label: "Routes" },
  { href: "/operations", label: "Operations" },
  { href: "/penalties", label: "Penalties" },
  { href: "/taxes", label: "Taxes" },
  { href: "/changes", label: "Changes" },
  { href: "/forecasting", label: "Forecasting" }
];
const ADMIN_NAV_ITEMS = [
  { href: "/admin", label: "Admin" },
  { href: "/health", label: "Health" }
];

function isActivePath(pathname: string, href: string) {
  if (href === "/") {
    return pathname === "/";
  }
  return pathname === href || pathname.startsWith(`${href}/`);
}

export function Topbar({
  showAdminLink = false,
  currentUserName,
  currentUserEmail
}: {
  showAdminLink?: boolean;
  currentUserName?: string | null;
  currentUserEmail?: string | null;
}) {
  const pathname = usePathname() || "/";
  const navItems = showAdminLink ? [...NAV_ITEMS, ...ADMIN_NAV_ITEMS] : NAV_ITEMS;

  async function signOut() {
    await fetch("/api/auth/logout", { method: "POST" });
    await nextAuthSignOut({ callbackUrl: "/" });
  }

  return (
    <div className="topbar">
      <div className="shell topbar-inner">
        <div className="brand">
          <div className="brand-mark">AP</div>
          <div>
            <div className="brand-title">Aero Pulse Intelligence Monitor</div>
            <div className="brand-subtitle">
              Route, operations, fare change, tax, penalty, and forecasting monitor
            </div>
          </div>
        </div>
        <nav className="nav" aria-label="Primary">
          {navItems.map((item) => (
            <Link
              key={item.href}
              className="nav-link"
              href={item.href}
              data-active={isActivePath(pathname, item.href)}
            >
              {item.label}
            </Link>
          ))}
        </nav>
        <div className="topbar-user">
          {currentUserEmail ? (
            <>
              <div className="topbar-user-copy">
                <strong>{currentUserName || "Signed in user"}</strong>
                <span>{currentUserEmail}</span>
              </div>
              <button className="button-link ghost topbar-user-button" onClick={signOut} type="button">
                Sign out
              </button>
            </>
          ) : (
            <Link className="button-link ghost topbar-user-button" href="/login">
              Sign in
            </Link>
          )}
        </div>
      </div>
    </div>
  );
}
