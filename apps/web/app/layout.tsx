import type { Metadata } from "next";
import { Space_Grotesk, IBM_Plex_Mono } from "next/font/google";
import { Analytics } from "@vercel/analytics/next";
import { SpeedInsights } from "@vercel/speed-insights/next";

import "./globals.css";

import { Topbar } from "@/components/topbar";

const display = Space_Grotesk({
  subsets: ["latin"],
  variable: "--font-display"
});

const mono = IBM_Plex_Mono({
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  variable: "--font-mono"
});

export const metadata: Metadata = {
  title: "Aero Pulse Intelligence Monitor",
  description: "Airline and OTA intelligence monitor for routes, operations, fare changes, taxes, penalties, and forecasting."
};

export default async function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  const vercelTelemetryEnabled = Boolean(process.env.VERCEL || process.env.NEXT_PUBLIC_VERCEL_ENV);
  return (
    <html lang="en" className={`${display.variable} ${mono.variable}`}>
      <body style={{ fontFamily: "var(--font-display)" }}>
        <a className="skip-link" href="#main-content">
          Skip to main content
        </a>
        <Topbar />
        <main id="main-content" tabIndex={-1} className="page shell">
          {children}
        </main>
        <footer
          style={{
            padding: "20px 24px",
            marginTop: "24px",
            borderTop: "1px solid rgba(0,0,0,0.08)",
            fontSize: "0.75rem",
            color: "var(--muted)",
            textAlign: "center",
          }}
        >
          &copy; 2026 Ihsan Kabir. All Rights Reserved. Proprietary software —
          see the{" "}
          <a
            href="https://github.com/IhsanKabir/Aviation-Inventory-Pricing-Intelligence-Using-CatBoost-LightGBM-MLP/blob/master/LICENSE"
            style={{ color: "inherit", textDecoration: "underline" }}
          >
            license
          </a>
          . Not for copying or redistribution.
        </footer>
        {vercelTelemetryEnabled ? <Analytics /> : null}
        {vercelTelemetryEnabled ? <SpeedInsights /> : null}
      </body>
    </html>
  );
}
