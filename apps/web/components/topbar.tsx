"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { signOut as nextAuthSignOut } from "next-auth/react";

const MARKET_ROOT_HREF = "/market";

const MARKET_ITEMS = [
  { href: "/routes", label: "Routes" },
  { href: "/operations", label: "Operations" },
  { href: "/penalties", label: "Penalties" },
  { href: "/taxes", label: "Taxes" },
  { href: "/changes", label: "Changes" },
  { href: "/gds", label: "GDS" },
];
const MARKET_HREFS = MARKET_ITEMS.map((item) => item.href);

const ADMIN_NAV_ITEMS = [
  { href: "/admin", label: "Admin" },
  { href: "/health", label: "Health" },
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
  currentUserEmail,
}: {
  showAdminLink?: boolean;
  currentUserName?: string | null;
  currentUserEmail?: string | null;
}) {
  const pathname = usePathname() || "/";
  const [sessionState, setSessionState] = useState({
    showAdminLink,
    currentUserName,
    currentUserEmail,
  });
  const adminItems = sessionState.showAdminLink ? ADMIN_NAV_ITEMS : [];
  const isMarketActive =
    isActivePath(pathname, MARKET_ROOT_HREF) ||
    MARKET_HREFS.some((href) => isActivePath(pathname, href));

  useEffect(() => {
    let active = true;

    async function loadSession() {
      try {
        const response = await fetch("/api/auth/session", { cache: "no-store" });
        if (!response.ok) {
          return;
        }
        const payload = (await response.json()) as {
          showAdminLink?: boolean;
          user?: { email?: string | null; full_name?: string | null } | null;
        };
        if (!active) {
          return;
        }
        setSessionState({
          showAdminLink: Boolean(payload.showAdminLink),
          currentUserName: payload.user?.full_name ?? null,
          currentUserEmail: payload.user?.email ?? null,
        });
      } catch {
        // Keep the optimistic signed-out shell when session lookup fails.
      }
    }

    loadSession();
    return () => {
      active = false;
    };
  }, []);

  async function signOut() {
    setSessionState({
      showAdminLink: false,
      currentUserName: null,
      currentUserEmail: null,
    });
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
          <Link className="nav-link" href="/" data-active={isActivePath(pathname, "/")}>
            Overview
          </Link>

          <div className="nav-group">
            <Link
              className="nav-link nav-group-link"
              href={MARKET_ROOT_HREF}
              data-active={isMarketActive}
              aria-haspopup="menu"
            >
              Market Intelligence
            </Link>
            <div
              className="nav-group-menu"
              id="market-intelligence-menu"
              aria-label="Market Intelligence pages"
              role="menu"
            >
              {MARKET_ITEMS.map((item) => (
                <Link
                  key={item.href}
                  className="nav-group-item"
                  href={item.href}
                  data-active={isActivePath(pathname, item.href)}
                  role="menuitem"
                >
                  {item.label}
                </Link>
              ))}
            </div>
          </div>

          <Link
            className="nav-link"
            href="/forecasting"
            data-active={isActivePath(pathname, "/forecasting")}
          >
            Forecasting
          </Link>
          <Link
            className="nav-link"
            href="/downloads"
            data-active={isActivePath(pathname, "/downloads")}
          >
            Downloads
          </Link>
          <Link
            className="nav-link"
            href="/usage"
            data-active={isActivePath(pathname, "/usage")}
          >
            Usage
          </Link>

          {adminItems.map((item) => (
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
          {sessionState.currentUserEmail ? (
            <>
              <div className="topbar-user-copy">
                <strong>{sessionState.currentUserName || "Signed in user"}</strong>
                <span>{sessionState.currentUserEmail}</span>
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
