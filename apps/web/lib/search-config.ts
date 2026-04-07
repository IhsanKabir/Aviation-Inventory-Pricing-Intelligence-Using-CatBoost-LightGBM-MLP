import "server-only";

import { execFile } from "node:child_process";
import { promises as fs } from "node:fs";
import { promisify } from "node:util";
import path from "node:path";

const CONFIG_ROOT = path.resolve(process.cwd(), "..", "..", "config");

const SCHEDULE_PATH = path.join(CONFIG_ROOT, "schedule.json");
const PASSENGER_PATH = path.join(CONFIG_ROOT, "passenger.json");
const OUTPUT_PATH = path.join(CONFIG_ROOT, "output.json");
const HOLIDAY_PATH = path.join(CONFIG_ROOT, "holiday_calendar.json");
const MARKET_PRIORS_PATH = path.join(CONFIG_ROOT, "market_priors.json");
const ROUTE_TRIP_WINDOWS_PATH = path.join(CONFIG_ROOT, "route_trip_windows.json");
const execFileAsync = promisify(execFile);

export type AdminHolidayEntry = {
  date: string;
  name: string;
  type: string;
  country: string;
  highDemand: boolean;
};

export type AdminTripProfileEntry = {
  key: string;
  description: string;
  tripType: string;
  archived: boolean;
  dayOffsets: string;
  dateRangesText: string;
  dayOffsetRangesText: string;
  returnDateOffsets: string;
  returnDateRangesText: string;
  returnDateOffsetRangesText: string;
};

export type AdminRouteProfileEntry = {
  key: string;
  airline: string;
  routeCode: string;
  marketTripProfiles: string[];
  activeMarketTripProfiles: string[];
  trainingMarketTripProfiles: string[];
  deepMarketTripProfiles: string[];
};

export type AdminSearchConfig = {
  schedule: {
    mode: string;
    concurrency: number;
    autoRunIntervalHours: number;
    defaultDateStart: string;
    defaultDateEnd: string;
    defaultDateOffsets: string;
    ingestionStartTime: string;
    trainingEnrichmentStartTime: string;
    trainingDeepStartTime: string;
    trainingDeepDayOfWeek: string;
  };
  passengers: {
    adt: number;
    chd: number;
    inf: number;
  };
  output: {
    csv: boolean;
    excel: boolean;
    json: boolean;
    archiveMode: string;
    filePrefix: string;
  };
  holidays: AdminHolidayEntry[];
  marketSummary: {
    laborOriginCountries: string[];
    middleEastDestinationCountries: string[];
    tourismAirports: string[];
    hubSpokeAirlines: string[];
    lccAirlines: string[];
    returnOrientedAirlines: string[];
    defaultOneWayOffsets: number[];
    tourismRoundTripOffsets: number[];
    holidayReturnOffsets: number[];
    tripProfileCount: number;
  };
  tripProfiles: AdminTripProfileEntry[];
  routeProfiles: AdminRouteProfileEntry[];
  advanced: {
    marketPriorsJson: string;
  };
  persistenceNote: string;
};

export type SchedulerApplyResult = {
  ok: boolean;
  platform: string;
  appliedAtUtc: string;
  steps: Array<{
    name: string;
    command: string;
    ok: boolean;
    output: string;
  }>;
};

type SearchConfigUpdate = {
  schedule: AdminSearchConfig["schedule"];
  passengers: AdminSearchConfig["passengers"];
  output: AdminSearchConfig["output"];
  holidays: AdminHolidayEntry[];
  tripProfiles: AdminTripProfileEntry[];
  routeProfiles: AdminRouteProfileEntry[];
  advanced: AdminSearchConfig["advanced"];
};

async function readJsonFile<T>(filePath: string): Promise<T> {
  const text = await fs.readFile(filePath, "utf-8");
  return JSON.parse(text) as T;
}

async function writeJsonFile(filePath: string, payload: unknown) {
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf-8");
}

function asPositiveInt(value: unknown, fallback: number) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(0, Math.trunc(parsed));
}

function normalizeOffsetCsv(value: unknown) {
  const pieces = String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  const deduped: string[] = [];
  const seen = new Set<string>();
  for (const piece of pieces) {
    if (!/^-?\d+$/.test(piece) || seen.has(piece)) {
      continue;
    }
    seen.add(piece);
    deduped.push(piece);
  }
  return deduped.join(", ");
}

function parseOffsetCsv(value: string) {
  return normalizeOffsetCsv(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => Number(item));
}

type DateRangeEntry = {
  start: string;
  end: string;
};

type OffsetRangeEntry = {
  start: number;
  end: number;
};

function formatDateRanges(value: unknown) {
  if (!Array.isArray(value)) {
    return "";
  }
  return value
    .map((item) => {
      const entry = item as Partial<DateRangeEntry>;
      const start = String(entry?.start || "").trim();
      const end = String(entry?.end || "").trim();
      if (!start || !end) {
        return "";
      }
      return `${start} to ${end}`;
    })
    .filter(Boolean)
    .join("\n");
}

function parseDateRangesText(value: string): DateRangeEntry[] {
  return String(value || "")
    .split(/\r?\n|;/)
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => {
      const normalized = item.replace(/\s+/g, " ");
      const parts = normalized.split(/\s+to\s+/i);
      if (parts.length !== 2) {
        throw new Error(`Invalid date range "${item}". Use YYYY-MM-DD to YYYY-MM-DD.`);
      }
      const start = parts[0]?.trim();
      const end = parts[1]?.trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end)) {
        throw new Error(`Invalid date range "${item}". Use YYYY-MM-DD to YYYY-MM-DD.`);
      }
      return { start, end };
    });
}

function formatOffsetRanges(value: unknown) {
  if (!Array.isArray(value)) {
    return "";
  }
  return value
    .map((item) => {
      const entry = item as Partial<OffsetRangeEntry>;
      const start = Number(entry?.start);
      const end = Number(entry?.end);
      if (!Number.isFinite(start) || !Number.isFinite(end)) {
        return "";
      }
      return `${Math.trunc(start)}-${Math.trunc(end)}`;
    })
    .filter(Boolean)
    .join(", ");
}

function parseOffsetRangesText(value: string): OffsetRangeEntry[] {
  return String(value || "")
    .split(/[\r\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => {
      const match = item.match(/^(-?\d+)\s*-\s*(-?\d+)$/);
      if (!match) {
        throw new Error(`Invalid offset range "${item}". Use forms like 0-7 or 14-30.`);
      }
      return {
        start: Number(match[1]),
        end: Number(match[2]),
      };
    });
}

function normalizeHolidayEntry(entry: Partial<AdminHolidayEntry>): AdminHolidayEntry {
  return {
    date: String(entry.date || "").trim(),
    name: String(entry.name || "").trim(),
    type: String(entry.type || "national").trim() || "national",
    country: String(entry.country || "BD").trim() || "BD",
    highDemand: Boolean(entry.highDemand),
  };
}

function buildMarketSummary(payload: any): AdminSearchConfig["marketSummary"] {
  const tripProfiles = payload?.trip_date_profiles && typeof payload.trip_date_profiles === "object"
    ? payload.trip_date_profiles
    : {};
  return {
    laborOriginCountries: Array.isArray(payload?.labor_flow_origin_countries) ? payload.labor_flow_origin_countries : [],
    middleEastDestinationCountries: Array.isArray(payload?.middle_east_destination_countries) ? payload.middle_east_destination_countries : [],
    tourismAirports: Array.isArray(payload?.thailand_tourism_airports) ? payload.thailand_tourism_airports : [],
    hubSpokeAirlines: Array.isArray(payload?.hub_spoke_airlines) ? payload.hub_spoke_airlines : [],
    lccAirlines: Array.isArray(payload?.lcc_airlines) ? payload.lcc_airlines : [],
    returnOrientedAirlines: Array.isArray(payload?.return_oriented_airlines) ? payload.return_oriented_airlines : [],
    defaultOneWayOffsets: Array.isArray(tripProfiles?.default_one_way_monitoring?.day_offsets)
      ? tripProfiles.default_one_way_monitoring.day_offsets
      : [],
    tourismRoundTripOffsets: Array.isArray(tripProfiles?.tourism_bkk_can_round_trip?.return_date_offsets)
      ? tripProfiles.tourism_bkk_can_round_trip.return_date_offsets
      : [],
    holidayReturnOffsets: Array.isArray(tripProfiles?.holiday_return_routes_next7_departure_plus3_grid?.return_date_offsets)
      ? tripProfiles.holiday_return_routes_next7_departure_plus3_grid.return_date_offsets
      : [],
    tripProfileCount: Object.keys(tripProfiles).length,
  };
}

function buildTripProfiles(payload: any): AdminTripProfileEntry[] {
  const tripProfiles = payload?.trip_date_profiles && typeof payload.trip_date_profiles === "object"
    ? payload.trip_date_profiles
    : {};
  return Object.entries(tripProfiles)
    .map(([key, value]) => {
      const profile = value as any;
      return {
        key,
        description: String(profile?.description || ""),
        tripType: String(profile?.trip_type || "OW"),
        archived: Boolean(profile?.archived),
        dayOffsets: normalizeOffsetCsv(Array.isArray(profile?.day_offsets) ? profile.day_offsets.join(",") : ""),
        dateRangesText: formatDateRanges(profile?.date_ranges),
        dayOffsetRangesText: formatOffsetRanges(profile?.day_offset_ranges),
        returnDateOffsets: normalizeOffsetCsv(
          Array.isArray(profile?.return_date_offsets) ? profile.return_date_offsets.join(",") : "",
        ),
        returnDateRangesText: formatDateRanges(profile?.return_date_ranges),
        returnDateOffsetRangesText: formatOffsetRanges(profile?.return_date_offset_ranges),
      };
    })
    .sort((left, right) => left.key.localeCompare(right.key));
}

function buildRouteProfiles(payload: any): AdminRouteProfileEntry[] {
  const airlines = payload?.airlines && typeof payload.airlines === "object" ? payload.airlines : {};
  const items: AdminRouteProfileEntry[] = [];
  for (const [airline, airlineConfig] of Object.entries(airlines)) {
    const routes = (airlineConfig as any)?.routes;
    if (!routes || typeof routes !== "object") {
      continue;
    }
    for (const [routeCode, routeConfig] of Object.entries(routes)) {
      const route = routeConfig as any;
      items.push({
        key: `${String(airline).toUpperCase()}::${String(routeCode).toUpperCase()}`,
        airline: String(airline).toUpperCase(),
        routeCode: String(routeCode).toUpperCase(),
        marketTripProfiles: Array.isArray(route?.market_trip_profiles) ? route.market_trip_profiles : [],
        activeMarketTripProfiles: Array.isArray(route?.active_market_trip_profiles) ? route.active_market_trip_profiles : [],
        trainingMarketTripProfiles: Array.isArray(route?.training_market_trip_profiles) ? route.training_market_trip_profiles : [],
        deepMarketTripProfiles: Array.isArray(route?.deep_market_trip_profiles) ? route.deep_market_trip_profiles : [],
      });
    }
  }
  return items.sort((left, right) => `${left.airline}-${left.routeCode}`.localeCompare(`${right.airline}-${right.routeCode}`));
}

export async function readAdminSearchConfig(): Promise<AdminSearchConfig> {
  const [schedule, passenger, output, holidayCalendar, marketPriors, routeTripWindows] = await Promise.all([
    readJsonFile<any>(SCHEDULE_PATH),
    readJsonFile<any>(PASSENGER_PATH),
    readJsonFile<any>(OUTPUT_PATH),
    readJsonFile<any>(HOLIDAY_PATH),
    readJsonFile<any>(MARKET_PRIORS_PATH),
    readJsonFile<any>(ROUTE_TRIP_WINDOWS_PATH),
  ]);
  const taskWindows = schedule?.task_windows && typeof schedule.task_windows === "object" ? schedule.task_windows : {};

  return {
    schedule: {
      mode: String(schedule?.mode || "manual"),
      concurrency: asPositiveInt(schedule?.concurrency, 1),
      autoRunIntervalHours: asPositiveInt(schedule?.auto_run_interval_hours, 6),
      defaultDateStart: String(schedule?.auto_run_date_ranges?.default?.date_start || ""),
      defaultDateEnd: String(schedule?.auto_run_date_ranges?.default?.date_end || ""),
      defaultDateOffsets: normalizeOffsetCsv(
        Array.isArray(schedule?.auto_run_date_ranges?.default?.date_offsets)
          ? schedule.auto_run_date_ranges.default.date_offsets.join(",")
          : schedule?.auto_run_date_ranges?.default?.date_offsets || "",
      ),
      ingestionStartTime: String(taskWindows?.ingestion?.start_time || "00:05"),
      trainingEnrichmentStartTime: String(taskWindows?.training_enrichment?.start_time || "01:30"),
      trainingDeepStartTime: String(taskWindows?.training_deep?.start_time || "02:00"),
      trainingDeepDayOfWeek: String(taskWindows?.training_deep?.day_of_week || "Sunday"),
    },
    passengers: {
      adt: asPositiveInt(passenger?.ADT, 1),
      chd: asPositiveInt(passenger?.CHD, 0),
      inf: asPositiveInt(passenger?.INF, 0),
    },
    output: {
      csv: Boolean(output?.formats?.csv),
      excel: Boolean(output?.formats?.excel),
      json: Boolean(output?.formats?.json),
      archiveMode: String(output?.archive_mode || "timestamp"),
      filePrefix: String(output?.file_prefix || "flights"),
    },
    holidays: Array.isArray(holidayCalendar?.holidays)
      ? holidayCalendar.holidays.map((item: any) =>
          normalizeHolidayEntry({
            date: item?.date,
            name: item?.name,
            type: item?.type,
            country: item?.country,
            highDemand: item?.high_demand,
          }),
        )
      : [],
    marketSummary: buildMarketSummary(marketPriors),
    tripProfiles: buildTripProfiles(marketPriors),
    routeProfiles: buildRouteProfiles(routeTripWindows),
    advanced: {
      marketPriorsJson: JSON.stringify(marketPriors, null, 2),
    },
    persistenceNote:
      "This editor changes the local repo config files used by the scraper on this machine. Hosted Vercel sessions can view the form, but bundled serverless files are not a reliable long-term place to save operational config.",
  };
}

export async function writeAdminSearchConfig(update: SearchConfigUpdate): Promise<AdminSearchConfig> {
  const [schedule, passenger, output, holidayCalendar, routeTripWindows] = await Promise.all([
    readJsonFile<any>(SCHEDULE_PATH),
    readJsonFile<any>(PASSENGER_PATH),
    readJsonFile<any>(OUTPUT_PATH),
    readJsonFile<any>(HOLIDAY_PATH),
    readJsonFile<any>(ROUTE_TRIP_WINDOWS_PATH),
  ]);

  let marketPriors: any;
  try {
    marketPriors = JSON.parse(String(update.advanced.marketPriorsJson || "").trim() || "{}");
  } catch (error) {
    throw new Error(`Market priors JSON is invalid: ${error instanceof Error ? error.message : "parse failed"}`);
  }
  if (!marketPriors || typeof marketPriors !== "object" || Array.isArray(marketPriors)) {
    throw new Error("Market priors JSON must be an object.");
  }

  schedule.mode = String(update.schedule.mode || "manual").trim() || "manual";
  schedule.concurrency = Math.max(1, asPositiveInt(update.schedule.concurrency, 1));
  schedule.auto_run_interval_hours = Math.max(1, asPositiveInt(update.schedule.autoRunIntervalHours, 6));
  schedule.auto_run_date_ranges = schedule.auto_run_date_ranges || {};
  schedule.auto_run_date_ranges.default = schedule.auto_run_date_ranges.default || {};
  schedule.auto_run_date_ranges.default.date_start = String(update.schedule.defaultDateStart || "").trim() || null;
  schedule.auto_run_date_ranges.default.date_end = String(update.schedule.defaultDateEnd || "").trim() || null;
  schedule.auto_run_date_ranges.default.date_offsets = parseOffsetCsv(update.schedule.defaultDateOffsets);
  schedule.task_windows = schedule.task_windows || {};
  schedule.task_windows.ingestion = schedule.task_windows.ingestion || {};
  schedule.task_windows.ingestion.start_time = String(update.schedule.ingestionStartTime || "00:05").trim() || "00:05";
  schedule.task_windows.training_enrichment = schedule.task_windows.training_enrichment || {};
  schedule.task_windows.training_enrichment.start_time =
    String(update.schedule.trainingEnrichmentStartTime || "01:30").trim() || "01:30";
  schedule.task_windows.training_deep = schedule.task_windows.training_deep || {};
  schedule.task_windows.training_deep.start_time =
    String(update.schedule.trainingDeepStartTime || "02:00").trim() || "02:00";
  schedule.task_windows.training_deep.day_of_week =
    String(update.schedule.trainingDeepDayOfWeek || "Sunday").trim() || "Sunday";

  passenger.ADT = Math.max(0, asPositiveInt(update.passengers.adt, 1));
  passenger.CHD = Math.max(0, asPositiveInt(update.passengers.chd, 0));
  passenger.INF = Math.max(0, asPositiveInt(update.passengers.inf, 0));

  output.formats = output.formats || {};
  output.formats.csv = Boolean(update.output.csv);
  output.formats.excel = Boolean(update.output.excel);
  output.formats.json = Boolean(update.output.json);
  output.archive_mode = String(update.output.archiveMode || "timestamp").trim() || "timestamp";
  output.file_prefix = String(update.output.filePrefix || "flights").trim() || "flights";

  holidayCalendar.holidays = (update.holidays || [])
    .map((item) => normalizeHolidayEntry(item))
    .filter((item) => item.date && item.name)
    .map((item) => ({
      date: item.date,
      name: item.name,
      type: item.type,
      country: item.country,
      high_demand: item.highDemand,
    }));

  marketPriors.trip_date_profiles = marketPriors.trip_date_profiles || {};
  for (const tripProfile of update.tripProfiles || []) {
    const key = String(tripProfile?.key || "").trim();
    if (!key) {
      continue;
    }
    if (!marketPriors.trip_date_profiles[key] || typeof marketPriors.trip_date_profiles[key] !== "object") {
      marketPriors.trip_date_profiles[key] = {};
    }
    marketPriors.trip_date_profiles[key].description = String(tripProfile.description || "").trim();
    marketPriors.trip_date_profiles[key].trip_type = String(tripProfile.tripType || "OW").trim() || "OW";
    marketPriors.trip_date_profiles[key].archived = Boolean(tripProfile.archived);
    marketPriors.trip_date_profiles[key].day_offsets = parseOffsetCsv(tripProfile.dayOffsets || "");
    marketPriors.trip_date_profiles[key].date_ranges = parseDateRangesText(tripProfile.dateRangesText || "");
    marketPriors.trip_date_profiles[key].day_offset_ranges = parseOffsetRangesText(tripProfile.dayOffsetRangesText || "");
    marketPriors.trip_date_profiles[key].return_date_offsets = parseOffsetCsv(tripProfile.returnDateOffsets || "");
    marketPriors.trip_date_profiles[key].return_date_ranges = parseDateRangesText(tripProfile.returnDateRangesText || "");
    marketPriors.trip_date_profiles[key].return_date_offset_ranges = parseOffsetRangesText(
      tripProfile.returnDateOffsetRangesText || "",
    );
  }

  routeTripWindows.airlines = routeTripWindows.airlines || {};
  for (const routeProfile of update.routeProfiles || []) {
    const airline = String(routeProfile.airline || "").trim().toUpperCase();
    const routeCode = String(routeProfile.routeCode || "").trim().toUpperCase();
    if (!airline || !routeCode) {
      continue;
    }
    const routeConfig = routeTripWindows?.airlines?.[airline]?.routes?.[routeCode];
    if (!routeConfig || typeof routeConfig !== "object") {
      continue;
    }
    routeConfig.market_trip_profiles = Array.from(
      new Set([...(routeProfile.marketTripProfiles || []), ...(routeProfile.activeMarketTripProfiles || [])]),
    );
    routeConfig.active_market_trip_profiles = routeProfile.activeMarketTripProfiles || [];
    routeConfig.training_market_trip_profiles = routeProfile.trainingMarketTripProfiles || [];
    routeConfig.deep_market_trip_profiles = routeProfile.deepMarketTripProfiles || [];
  }

  await Promise.all([
    writeJsonFile(SCHEDULE_PATH, schedule),
    writeJsonFile(PASSENGER_PATH, passenger),
    writeJsonFile(OUTPUT_PATH, output),
    writeJsonFile(HOLIDAY_PATH, holidayCalendar),
    writeJsonFile(MARKET_PRIORS_PATH, marketPriors),
    writeJsonFile(ROUTE_TRIP_WINDOWS_PATH, routeTripWindows),
  ]);

  return readAdminSearchConfig();
}

export async function applySchedulerSettingsOnMachine(): Promise<SchedulerApplyResult> {
  if (process.platform !== "win32") {
    throw new Error("This action only works on the Windows machine that owns the local scheduler tasks.");
  }

  const repoRoot = path.resolve(process.cwd(), "..", "..");
  const scripts = [
    "scheduler\\install_ingestion_autorun.ps1",
    "scheduler\\install_training_enrichment_autorun.ps1",
    "scheduler\\install_training_deep_autorun.ps1",
  ];

  const steps: SchedulerApplyResult["steps"] = [];
  for (const relativeScript of scripts) {
    const scriptPath = path.join(repoRoot, relativeScript);
    const command = `powershell.exe -NoProfile -ExecutionPolicy Bypass -File "${scriptPath}"`;
    try {
      const { stdout, stderr } = await execFileAsync(
        "powershell.exe",
        ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", scriptPath],
        {
          cwd: repoRoot,
          windowsHide: true,
          timeout: 120000,
          maxBuffer: 1024 * 1024,
        },
      );
      steps.push({
        name: path.basename(relativeScript),
        command,
        ok: true,
        output: [stdout, stderr].filter(Boolean).join("\n").trim(),
      });
    } catch (error) {
      const detail =
        error && typeof error === "object" && "stdout" in error
          ? [String((error as { stdout?: unknown }).stdout || ""), String((error as { stderr?: unknown }).stderr || "")]
              .filter(Boolean)
              .join("\n")
              .trim()
          : error instanceof Error
            ? error.message
            : "Unknown scheduler apply failure";
      steps.push({
        name: path.basename(relativeScript),
        command,
        ok: false,
        output: detail,
      });
      throw new Error(`Scheduler apply failed in ${path.basename(relativeScript)}: ${detail || "unknown error"}`);
    }
  }

  return {
    ok: true,
    platform: process.platform,
    appliedAtUtc: new Date().toISOString(),
    steps,
  };
}
