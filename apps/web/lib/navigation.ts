/**
 * Validates a user-supplied return path (e.g. the `?next=` query param used by
 * the login flows) so redirects can never leave this origin.
 *
 * Allows only same-origin absolute paths like "/routes?x=1".
 * Rejects external URLs ("https://evil.com"), protocol-relative URLs
 * ("//evil.com"), backslash variants ("/\\evil.com" — WHATWG URL parsing
 * treats "\" as "/" and reads an authority), and control characters
 * (NUL bytes survive trim() and can confuse downstream parsers).
 */
export function sanitizeReturnPath(value: string | null | undefined, fallback: string): string {
  if (!value) {
    return fallback;
  }
  const trimmed = value.trim();
  for (let index = 0; index < trimmed.length; index += 1) {
    const code = trimmed.charCodeAt(index);
    if (code <= 0x1f || code === 0x7f) {
      return fallback;
    }
  }
  if (!trimmed.startsWith("/") || trimmed.startsWith("//") || trimmed.charAt(1) === "\\") {
    return fallback;
  }
  // Final authority check: parse against a fixed origin — if the candidate can
  // escape it (e.g. "/\\evil.com" parsing as host evil.com), reject it.
  try {
    const parsed = new URL(trimmed, "http://internal");
    if (parsed.origin !== "http://internal" || !parsed.pathname.startsWith("/")) {
      return fallback;
    }
  } catch {
    return fallback;
  }
  return trimmed;
}
