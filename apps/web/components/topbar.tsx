"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useRef, useState } from "react";
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
  const adminItems = showAdminLink ? ADMIN_NAV_ITEMS : [];
  const marketMenuRef = useRef<HTMLDivElement>(null);
  const [isMarketMenuOpen, setIsMarketMenuOpen] = useState(false);
  const isMarketActive =
    isActivePath(pathname, MARKET_ROOT_HREF) ||
    MARKET_HREFS.some((href) => isActivePath(pathname, href));

  useEffect(() => {
    setIsMarketMenuOpen(false);
  }, [pathname]);

  useEffect(() => {
    function handlePointerDown(event: PointerEvent) {
      if (!marketMenuRef.current) {
        return;
      }
      if (event.target instanceof Node && !marketMenuRef.current.contains(event.target)) {
        setIsMarketMenuOpen(false);
      }
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setIsMarketMenuOpen(false);
      }
    }

    window.addEventListener("pointerdown", handlePointerDown);
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("pointerdown", handlePointerDown);
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, []);

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
          <Link className="nav-link" href="/" data-active={isActivePath(pathname, "/")}>
            Overview
          </Link>

          <div className="nav-group" ref={marketMenuRef} data-open={isMarketMenuOpen}>
            <Link
              className="nav-link nav-group-link"
              href={MARKET_ROOT_HREF}
              data-active={isMarketActive}
            >
              Market Intelligence
            </Link>
            <button
              aria-controls="market-intelligence-menu"
              aria-expanded={isMarketMenuOpen}
              aria-haspopup="true"
              aria-label="Open Market Intelligence pages"
              className="nav-group-toggle"
              data-active={isMarketActive}
              onClick={() => setIsMarketMenuOpen((open) => !open)}
              type="button"
            >
              <span aria-hidden="true">{isMarketMenuOpen ? "^" : "v"}</span>
            </button>
            <div
              className="nav-group-menu"
              id="market-intelligence-menu"
              aria-label="Market Intelligence pages"
            >
              {MARKET_ITEMS.map((item) => (
                <Link
                  key={item.href}
                  className="nav-group-item"
                  href={item.href}
                  data-active={isActivePath(pathname, item.href)}
                  onClick={() => setIsMarketMenuOpen(false)}
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
