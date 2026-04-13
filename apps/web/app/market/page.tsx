import Link from "next/link";

import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getDashboardPayload } from "@/lib/api";

export const dynamic = "force-dynamic";

export const metadata = {
  title: "Market Intelligence | Aero Pulse Intelligence Monitor",
  description: "Live route, operations, changes, tax, penalty, and GDS intelligence workspace.",
};

const MARKET_VIEWS = [
  {
    href: "/routes",
    label: "Routes",
    desc: "Compare live fare snapshots by route, carrier, cabin, and travel window.",
    tags: ["Fare monitor", "Route comparison"],
  },
  {
    href: "/operations",
    label: "Operations",
    desc: "Review who is flying, how often they fly, and how the route footprint is shifting.",
    tags: ["Schedule patterns", "Frequency"],
  },
  {
    href: "/changes",
    label: "Changes",
    desc: "Scan price, availability, schedule, tax, and penalty events across the market.",
    tags: ["Movement dashboard", "Drilldown"],
  },
  {
    href: "/taxes",
    label: "Taxes",
    desc: "Inspect tax composition, route spread, and current airline-level tax differences.",
    tags: ["Tax monitor", "Route spread"],
  },
  {
    href: "/penalties",
    label: "Penalties",
    desc: "Check penalty rules and compare the current fare-rule posture by market and carrier.",
    tags: ["Fare rules", "Comparison"],
  },
  {
    href: "/gds",
    label: "GDS",
    desc: "Track Smartpoint-originated fare and tax intelligence as a distinct source view.",
    tags: ["Travelport", "Source specific"],
  },
];

const QUESTION_GUIDE = [
  {
    href: "/routes",
    question: "Which route and airline combinations are moving in price right now?",
    destination: "Routes",
    note: "Use the live route monitor when you need route-first fare comparison.",
  },
  {
    href: "/operations",
    question: "Who is flying, when are they flying, and how dense is the schedule?",
    destination: "Operations",
    note: "Use the operations view for weekly rhythm, timing, and route presence.",
  },
  {
    href: "/changes",
    question: "What changed across the market today or over the last few days?",
    destination: "Changes",
    note: "Use the changes view for scanning, direction split, and row-level event review.",
  },
  {
    href: "/gds",
    question: "Do I need the GDS-only picture from Smartpoint data?",
    destination: "GDS",
    note: "Use the GDS view for source-specific fare and tax intelligence.",
  },
];

function formatDate(value?: string | null) {
  if (!value) {
    return "Not available";
  }
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Asia/Dhaka",
  }).format(new Date(value));
}

export default async function MarketIntelligencePage() {
  const payload = await getDashboardPayload();
  const latestCycle = payload.latestCycle.data;
  const cycleHealth = payload.cycleHealth.data;

  return (
    <>
      <h1 className="page-title">Market Intelligence</h1>
      <p className="page-copy">
        Use this workspace for the live market picture: route pricing, airline operations,
        market changes, taxes, penalties, and GDS-originated fare intelligence. Start here,
        then drill into the page that matches the question you need answered.
      </p>

      <div className="grid cards">
        <MetricCard
          label="Latest update"
          value={formatDate(latestCycle?.cycle_completed_at_utc)}
          footnote="Most recent completed market snapshot"
        />
        <MetricCard
          label="Coverage"
          value={`${cycleHealth?.route_pair_coverage_pct?.toFixed(1) ?? "0.0"}%`}
          footnote={`${(cycleHealth?.observed_route_pair_count ?? 0).toLocaleString()} active route-airline pairs`}
        />
        <MetricCard
          label="Airlines"
          value={latestCycle?.airline_count?.toLocaleString() ?? "0"}
          footnote="Distinct carriers in the current market slice"
        />
        <MetricCard
          label="Routes"
          value={latestCycle?.route_count?.toLocaleString() ?? "0"}
          footnote="Origin-destination pairs available in the latest update"
        />
      </div>

      <div className="section-grid">
        <DataPanel
          title="Included views"
          copy="Choose the live market page that matches the slice of intelligence you need."
        >
          <div className="market-view-grid">
            {MARKET_VIEWS.map((view, index) => (
              <Link
                href={view.href}
                className="card roadmap-step market-view-card"
                key={view.href}
                style={{ textDecoration: "none", color: "inherit" }}
              >
                <div className="roadmap-step-header">
                  <div className="step-number">{index + 1}</div>
                  <div className="market-view-headline">
                    <strong>{view.label}</strong>
                    <span className="market-view-tagline">{view.tags.join(" | ")}</span>
                  </div>
                  <div className="nav-card-arrow">{">"}</div>
                </div>
                <div className="roadmap-step-desc market-view-desc">{view.desc}</div>
                <div className="market-view-pills">
                  {view.tags.map((tag) => (
                    <span className="market-view-pill" key={`${view.href}-${tag}`}>
                      {tag}
                    </span>
                  ))}
                </div>
              </Link>
            ))}
          </div>
        </DataPanel>

        <DataPanel
          title="Start with the question"
          copy="If you are not sure where to go first, use the question that best matches your task."
        >
          <div className="table-list">
            {QUESTION_GUIDE.map((item) => (
              <Link
                href={item.href}
                className="table-row market-guide-row"
                key={item.href}
                style={{ textDecoration: "none", color: "inherit" }}
              >
                <div>
                  <strong>{item.question}</strong>
                  <span>{item.note}</span>
                </div>
                <div className="pill good">{item.destination}</div>
                <span>Open view</span>
              </Link>
            ))}
          </div>
        </DataPanel>
      </div>
    </>
  );
}
