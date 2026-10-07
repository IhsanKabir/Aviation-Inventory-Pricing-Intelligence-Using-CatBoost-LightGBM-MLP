/**
 * Market Intelligence and Forecasting are paused to save running costs: their data comes
 * from BigQuery (billed per query) and the collection pipeline has been stopped since
 * July 2026. While paused, the pages show a notice and make no data requests.
 *
 * To bring them back, set MARKET_DATA_ENABLED=true on Vercel AND on the API (Cloud Run).
 */
export function marketDataEnabled(): boolean {
  return (process.env.MARKET_DATA_ENABLED ?? "").trim().toLowerCase() === "true";
}
