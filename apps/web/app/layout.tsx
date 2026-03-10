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
  description: "Hosted airline and OTA intelligence monitor with warehouse-backed reporting and forecasting."
};

export default async function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${display.variable} ${mono.variable}`}>
      <body style={{ fontFamily: "var(--font-display)" }}>
        <Topbar />
        <main className="page shell">{children}</main>
        <Analytics />
        <SpeedInsights />
      </body>
    </html>
  );
}
