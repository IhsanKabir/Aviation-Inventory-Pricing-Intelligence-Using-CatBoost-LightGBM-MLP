import "server-only";

import { createHash, timingSafeEqual } from "node:crypto";

import { cookies } from "next/headers";
import { redirect } from "next/navigation";

const ADMIN_SESSION_COOKIE = "ap_admin_session";
const DEFAULT_ADMIN_USERNAME = "admin";

function sha256Hex(value: string) {
  return createHash("sha256").update(value).digest("hex");
}

function getAdminUsername() {
  return (process.env.WEB_ADMIN_USERNAME || DEFAULT_ADMIN_USERNAME).trim() || DEFAULT_ADMIN_USERNAME;
}

function getAdminPassword() {
  return (
    process.env.WEB_ADMIN_PASSWORD?.trim() ||
    process.env.REPORT_ACCESS_ADMIN_TOKEN?.trim() ||
    ""
  );
}

function getAdminSessionSecret() {
  return (
    process.env.WEB_ADMIN_SESSION_SECRET?.trim() ||
    process.env.WEB_ADMIN_PASSWORD?.trim() ||
    process.env.REPORT_ACCESS_ADMIN_TOKEN?.trim() ||
    ""
  );
}

function getExpectedSessionToken() {
  const secret = getAdminSessionSecret();
  if (!secret) {
    return "";
  }
  return sha256Hex(`ap-admin::${secret}`);
}

function safeEqual(left: string, right: string) {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }
  return timingSafeEqual(leftBuffer, rightBuffer);
}

export function isAdminConfigured() {
  return Boolean(getAdminPassword() && getAdminSessionSecret());
}

export function verifyAdminCredentials(username: string, password: string) {
  if (!isAdminConfigured()) {
    return false;
  }
  return safeEqual(username.trim(), getAdminUsername()) && safeEqual(password, getAdminPassword());
}

export function buildAdminSessionCookieValue() {
  return getExpectedSessionToken();
}

export async function hasAdminSession() {
  const cookieStore = await cookies();
  const token = cookieStore.get(ADMIN_SESSION_COOKIE)?.value || "";
  const expected = getExpectedSessionToken();
  return Boolean(token && expected && safeEqual(token, expected));
}

export async function requireAdminSession(returnTo = "/admin") {
  const adminSession = await hasAdminSession();
  if (adminSession) {
    return true;
  }
  const encoded = encodeURIComponent(returnTo);
  redirect(`/admin/login${encoded ? `?next=${encoded}` : ""}`);
}

export function getAdminSessionCookieName() {
  return ADMIN_SESSION_COOKIE;
}

