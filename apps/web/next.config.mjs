/** @type {import('next').NextConfig} */

// The API origin the browser talks to — needed in connect-src. Falls back to the
// known Cloud Run host so the CSP is correct even if the env var isn't set at build.
const API_ORIGIN = (() => {
  const raw = process.env.NEXT_PUBLIC_API_BASE_URL || process.env.API_BASE_URL || "";
  try {
    return raw ? new URL(raw).origin : "https://aero-pulse-api-591603094460.asia-south1.run.app";
  } catch {
    return "https://aero-pulse-api-591603094460.asia-south1.run.app";
  }
})();

// Shipped Report-Only first: the app uses many inline styles + NextAuth, so
// enforcing immediately would break the UI. Report-Only surfaces violations
// without blocking, so we can tighten to an enforcing CSP once reports are clean.
const CSP = [
  "default-src 'self'",
  "base-uri 'self'",
  "object-src 'none'",
  "frame-ancestors 'none'",
  "form-action 'self'",
  "img-src 'self' data: https:",
  "font-src 'self' https://fonts.gstatic.com data:",
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
  "script-src 'self' 'unsafe-inline' 'unsafe-eval'",
  `connect-src 'self' ${API_ORIGIN} https://vitals.vercel-insights.com https://accounts.google.com`,
  "frame-src 'self' https://accounts.google.com",
].join("; ");

const SECURITY_HEADERS = [
  { key: "Strict-Transport-Security", value: "max-age=31536000; includeSubDomains; preload" },
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "X-Frame-Options", value: "DENY" },
  { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
  { key: "Permissions-Policy", value: "camera=(), microphone=(), geolocation=()" },
  { key: "Content-Security-Policy-Report-Only", value: CSP },
];

const nextConfig = {
  reactStrictMode: true,
  async headers() {
    return [
      { source: "/:path*", headers: SECURITY_HEADERS },
      {
        source: "/_next/static/:path*",
        headers: [{ key: "Cache-Control", value: "public, max-age=31536000, immutable" }],
      },
    ];
  },
};

export default nextConfig;
