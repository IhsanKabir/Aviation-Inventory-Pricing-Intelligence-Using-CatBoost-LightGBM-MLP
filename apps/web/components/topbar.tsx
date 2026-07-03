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
  { href: "/discount-comparison", label: "OTA Discounts" },
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
  // Menu visibility is React state (not CSS :hover) so the submenu stays reachable
  // for keyboard, touch, and assistive-tech users; hover-only menus exclude all three.
  const [marketOpen, setMarketOpen] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const marketGroupRef = useRef<HTMLDivElement | null>(null);
  const marketToggleRef = useRef<HTMLButtonElement | null>(null);
  const adminItems = sessionState.showAdminLink ? ADMIN_NAV_ITEMS : [];
  const isMarketActive =
    isActivePath(pathname, MARKET_ROOT_HREF) ||
    MARKET_HREFS.some((href) => isActivePath(pathname, href));

  useEffect(() => {
    setMarketOpen(false);
    setMobileNavOpen(false);
  }, [pathname]);

  useEffect(() => {
    if (!marketOpen) {
      return;
    }

    function handlePointerDown(event: PointerEvent) {
      const group = marketGroupRef.current;
      if (group && event.target instanceof Node && !group.contains(event.target)) {
        setMarketOpen(false);
      }
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setMarketOpen(false);
        // Return focus to the disclosure control so keyboard users aren't stranded.
        marketToggleRef.current?.focus();
      }
    }

    document.addEventListener("pointerdown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [marketOpen]);

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

  function navLinkProps(href: string) {
    const active = isActivePath(pathname, href);
    return {
      "data-active": active,
      "aria-current": active ? ("page" as const) : undefined,
    };
  }

  function handleMarketBlur(event: React.FocusEvent<HTMLDivElement>) {
    if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
      setMarketOpen(false);
    }
  }

  return (
    <header className="topbar">
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
        <button
          className="nav-toggle"
          type="button"
          aria-expanded={mobileNavOpen}
          aria-controls="primary-nav"
          onClick={() => setMobileNavOpen((open) => !open)}
        >
          <span aria-hidden="true">{mobileNavOpen ? "✕" : "☰"}</span>
          Menu
        </button>
        <nav className="nav" id="primary-nav" aria-label="Primary" data-open={mobileNavOpen}>
          <Link className="nav-link" href="/" {...navLinkProps("/")}>
            Overview
          </Link>

          <div
            className="nav-group"
            ref={marketGroupRef}
            data-open={marketOpen}
            onPointerEnter={(event) => {
              if (event.pointerType === "mouse") {
                setMarketOpen(true);
              }
            }}
            onPointerLeave={(event) => {
              if (event.pointerType === "mouse") {
                setMarketOpen(false);
              }
            }}
            onBlur={handleMarketBlur}
          >
            <Link
              className="nav-link nav-group-link"
              href={MARKET_ROOT_HREF}
              data-active={isMarketActive}
              aria-current={isActivePath(pathname, MARKET_ROOT_HREF) ? "page" : undefined}
            >
              Market Intelligence
            </Link>
            <button
              className="nav-group-toggle"
              type="button"
              ref={marketToggleRef}
              aria-expanded={marketOpen}
              aria-controls="market-intelligence-menu"
              onClick={() => setMarketOpen((open) => !open)}
            >
              <span aria-hidden="true">▾</span>
              <span className="sr-only">Toggle Market Intelligence pages</span>
            </button>
            <div className="nav-group-menu" id="market-intelligence-menu">
              {MARKET_ITEMS.map((item) => (
                <Link
                  key={item.href}
                  className="nav-group-item"
                  href={item.href}
                  {...navLinkProps(item.href)}
                >
                  {item.label}
                </Link>
              ))}
            </div>
          </div>

          <Link className="nav-link" href="/forecasting" {...navLinkProps("/forecasting")}>
            Forecasting
          </Link>
          <Link className="nav-link" href="/downloads" {...navLinkProps("/downloads")}>
            Downloads
          </Link>
          <Link className="nav-link" href="/usage" {...navLinkProps("/usage")}>
            Usage
          </Link>
          <Link className="nav-link" href="/account" {...navLinkProps("/account")}>
            Account
          </Link>

          {adminItems.map((item) => (
            <Link key={item.href} className="nav-link" href={item.href} {...navLinkProps(item.href)}>
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
    </header>
  );
}
