# OTA Discount Report — HAR Collection Guide

How to capture the HAR files so `tools/ota_discount_grid.py` can build the full
DOM/INTL commission grid.

## The loop

1. Capture a HAR from each site (steps below).
2. Drop all HARs into **`output\discount_hars\`** (replace yesterday's).
3. Run:
   ```powershell
   python tools/ota_discount_grid.py --auto --routes DAC-CGP,DAC-DXB,DAC-SIN --date 2026-07-30
   ```
4. Read the output in `output\reports\`:
   - **`OTA_Discount_Grid.xlsx`** — the rolling workbook; each run adds a sheet
     named for the run date (`29 June` style), in the colored **best-discount**
     format: OTA names as rows, airlines as columns, **INTERNATIONAL** and
     **DOMESTIC** as separate stacked blocks. Each block has a merged title with the
     legend `Red = Change, Green = Highest, Blue = Second Place`, a **yellow** header
     row, and per-airline highlighting **within each B2B / B2C group** — 🟢 green =
     highest discount, 🔵 blue = second, 🔴 red = changed vs the previous day's sheet.
     Values are percentages; a coupon cell (`9(Bkash), 18 (EBL)`) is stored as its
     leading common rate with the full text kept as a cell **comment**. Re-running the
     same day **overwrites** that day's sheet. This is the daily report.
   - A timestamped side-by-side **CSV + JSON** snapshot of the same run.

   (Pass `--no-xlsx` to skip the workbook, or `--xlsx <path>` to target a
   different one.)

> **Dates are flexible.** HAR channels ignore `--date` entirely — each HAR carries
> its own search date, so you can search different dates on different sites and it
> just works. `--date` is **optional** and only sets the travel date for the **live**
> FirstTrip B2C fetch (`--routes`). For different dates per route, attach them:
> `--routes DAC-CGP@2026-07-30,DAC-DXB@2026-08-01`. The report header shows the
> run timestamp (e.g. `28/06/2026 / 1458hrs`), like the original report.

## Actual discount % (true base — ON by default)

Every cell is a **percentage off the base fare**. The catch: not every channel
reports the base the same way, so a naive % isn't comparable across channels.

- **FirstTrip B2B / B2C and Amy** report the *real* base — and they all agree on it
  for the same airline + flight. Domestically the tax is a **fixed per-airline
  amount** (BS/2A/VQ ≈ 1125 BDT, BG ≈ 1225 BDT), so `base = gross − tax`.
- **BDFare** only exposes totals, so it *estimates* base as `gross × ratio`. That
  ratio undershoots the real base (more on pricier fares), which **overstates** its %.
- **AKIJ** reclassifies ~300 BDT of base into "tax", giving a smaller base and again
  **overstating** its %.

So by default the grid learns the **true base** per `(airline, gross)` from the
exact-base channels and recomputes **BDFare** and **AKIJ** *domestic* cells on it —
the report shows the **actual %**. Example: BDFare 2A goes from a ratio-inflated
`13.85` to the true `13.0` (matching the manual report). The B2B cell is the **agent
margin** `(customerNet − agent) / true_base`.

- This is **on automatically** — the daily command above already produces actual %s.
- International cells are left as-is (intl tax varies, so there's no fixed-tax law).
- If a run has no FirstTrip B2B HAR and no domestic route to learn from, it safely
  falls back to each channel's own base (cells are never blanked).
- Pass **`--no-true-base`** to see the raw (channel-reported) %s instead.

### Audit: how was each base altered?

To see *which* channels altered their base and the actual discount/markup behind each
cell, run the read-only audit alongside the report:

```powershell
python tools/base_fare_audit.py --routes DAC-CGP --date 2026-07-30
```

It prints the true base per `(airline, gross)`, flags each BDFare/AKIJ cell as
`base OK` or `ALTERED by ±N`, and shows the agent discount vs customer markup. A JSON
copy lands in `output\reports\base_fare_audit_*.json`.

## Golden rule for exporting a HAR

In Chrome DevTools (F12) → **Network** tab:

1. Tick **Preserve log** BEFORE you search.
2. Log in to the site first (if it needs an account).
3. Do the search (and for GoZayaan/ShareTrip, continue to the booking page — see below).
4. Wait until results **fully finish loading** (spinner stops).
5. Right-click in the Network list → **Save all as HAR with content**.
   ⚠️ "**with content**" is essential — without it the response bodies are empty
   and the parser sees nothing (this is why an earlier AKIJ HAR came back blank).
6. Save with the site name in the filename (so `--auto` recognizes it).

## Per-channel capture

| Channel | Site | Login? | What to do | Where the data is | Filename must contain |
|---|---|---|---|---|---|
| **FirstTrip B2C** | — | — | nothing — fetched **live** by `--routes` | live API | (none) |
| **FirstTrip B2B** → USBA row | booking.firsttrip.com | yes (agent) | search route/date; **wait for all airlines to finish loading** | search (Progressive) | `booking.firsttrip` |
| **BDFare** | bdfare.com/searchpad | yes (agent) | search route/date; optionally click a flight (improves base-fare accuracy) | search (AirSearch) | `bdfare` |
| **Amy** | amyweb.amybd.com (agent) | yes (agent) | search route/date | search (api.aspx) | `amyweb` or `amybd` |
| **AKIJ Air** | akijair.com | yes (Google) | search route/date; wait for results | search (/flight/search) | `akij` |
| **ShareTrip B2C** | sharetrip.net | recommended | search route/date (gets the **common** rate). For bKash/EBL specifics: select a flight → **booking page → DISCOUNT COUPON** | search = common; booking = specifics | `sharetrip` |
| **GoZayaan** | gozayaan.com | yes | search route/date, then **select a flight → booking/payment page** (the coupon list loads there) | **booking page only** | `gozayaan` |

### Notes that matter

- **GoZayaan needs the booking page.** The discount %s come from `get_discount_list`,
  which only fires after you select a flight and reach the booking/payment screen.
  Just viewing the search results is **not** enough. The coupons are payment-based
  (bKash, EBL, AMEX…) and largely the same across airlines for a given DOM/INTL
  route, so one booking view per route-type usually covers it — but the tool fills a
  cell only for the airline whose booking you opened, so open one per airline you care about.
- **ShareTrip**: the search page alone gives the *common* rate (e.g. BS 7%). The
  `9(Bkash), 18(EBL)` specifics need the booking page's DISCOUNT COUPON list.
- **FirstTrip B2B streams airlines in batches** — wait for the spinner to fully
  stop before exporting, or some carriers (2A/VQ) will be missing.
- **One route per HAR is fine**, but capturing both a domestic and an
  international search in the same session gives you both grids from one file.

## ⛔ DON'T capture everything into ONE big HAR

**This is a trap — verified to silently lose data.** In a long, many-site session,
Chrome DevTools **evicts older response bodies** to save memory. When you export, the
requests are still listed (status 200) but their **response bodies are empty** — so the
parsers see nothing. In one real 234 MB combined HAR, only the *last* site captured
(GoZayaan) kept its bodies; BDFare/AKIJ/FirstTrip/Amy/ShareTrip all came back blank even
though the searches (incl. international routes) had been done.

**Always export per-site instead:**

1. Search **one site**, then **immediately** "Save all as HAR with content" → e.g. `bdfare.com.har`.
2. Clear the network log (🚫), do the **next** site, export it.
3. Drop each per-site HAR in `output\discount_hars\` and run `--auto`.

Exporting right after each site means its bodies are still in DevTools' buffer.

`--combined-har "file.har"` exists for a genuinely **short, single** capture that holds
everything — but for a normal multi-site daily run, **per-site + `--auto` is the only
reliable path.**

Sequence for one combined HAR:
1. One tab, F12 → Network → **Preserve log** on.
2. Log into each portal as you go.
3. Visit each site, search your route(s), let results fully load.
4. GoZayaan/ShareTrip: open booking, copy the URL, paste back into the recording tab.
5. End: right-click Network → **Save all as HAR with content** → one file.
6. Run the `--combined-har` command above.

## Coverage = which routes to search

- **Domestic grid** (BS, 2A, BG, VQ): search a domestic route that carries all four,
  e.g. `DAC-CGP` or `DAC-CXB`.
- **International grid**: search your key international routes (e.g. `DAC-DXB`,
  `DAC-SIN`, `DAC-JED`). Each search adds that route's airlines to the INTL columns.

## Troubleshooting

- **A whole channel is blank** → its HAR is missing, misnamed, or exported without
  content. Check the filename hint, and re-export with **"Save all as HAR with content"**.
- **GoZayaan/ShareTrip discount blank but price present** → you didn't reach the
  booking page; redo and continue to booking.
- **An airline column is blank for a B2B channel** → that portal didn't return that
  airline (e.g. FirstTrip B2B doesn't sell VQ), or the search hadn't finished loading.
- **Verify a single HAR** before a full run:
  ```powershell
  python tools/ota_discount_grid.py --auto C:\path\to\folder --date 2026-07-30
  ```
  Watch the `detected ...` lines to confirm each file maps to the right channel.
