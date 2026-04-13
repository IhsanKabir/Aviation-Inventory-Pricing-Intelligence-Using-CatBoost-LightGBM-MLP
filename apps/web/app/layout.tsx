import type { Metadata } from "next";
import { Space_Grotesk, IBM_Plex_Mono } from "next/font/google";
import { Analytics } from "@vercel/analytics/next";
import { SpeedInsights } from "@vercel/speed-insights/next";

import "./globals.css";

import { hasAdminSession } from "@/lib/admin";
import { Topbar } from "@/components/topbar";
import { getCurrentUserSession } from "@/lib/user-auth";

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
  const showAdminLink = await hasAdminSession();
  const { user } = await getCurrentUserSession();
  return (
    <html lang="en" className={`${display.variable} ${mono.variable}`}>
      <body style={{ fontFamily: "var(--font-display)" }}>
        <Topbar
          currentUserEmail={user?.email ?? null}
          currentUserName={user?.full_name ?? null}
          showAdminLink={showAdminLink}
        />
        <main className="page shell">{children}</main>
        {vercelTelemetryEnabled ? <Analytics /> : null}
        {vercelTelemetryEnabled ? <SpeedInsights /> : null}
      </body>
    </html>
  );
}
