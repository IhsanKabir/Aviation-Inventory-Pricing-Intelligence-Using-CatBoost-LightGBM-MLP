import "server-only";

import { promises as fs } from "node:fs";
import path from "node:path";

const CONFIG_ROOT = path.resolve(process.cwd(), "..", "..", "config");

const SCHEDULE_PATH = path.join(CONFIG_ROOT, "schedule.json");
const PASSENGER_PATH = path.join(CONFIG_ROOT, "passenger.json");
const OUTPUT_PATH = path.join(CONFIG_ROOT, "output.json");
const HOLIDAY_PATH = path.join(CONFIG_ROOT, "holiday_calendar.json");
const MARKET_PRIORS_PATH = path.join(CONFIG_ROOT, "market_priors.json");

export type AdminHolidayEntry = {
  date: string;
  name: string;
  type: string;
  country: string;
  highDemand: boolean;
};

export type AdminSearchConfig = {
  schedule: {
    mode: string;
    concurrency: number;
    autoRunIntervalHours: number;
    defaultDateStart: string;
    defaultDateEnd: string;
    defaultDateOffsets: string;
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
  advanced: {
    marketPriorsJson: string;
  };
  persistenceNote: string;
};

type SearchConfigUpdate = {
  schedule: AdminSearchConfig["schedule"];
  passengers: AdminSearchConfig["passengers"];
  output: AdminSearchConfig["output"];
  holidays: AdminHolidayEntry[];
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

export async function readAdminSearchConfig(): Promise<AdminSearchConfig> {
  const [schedule, passenger, output, holidayCalendar, marketPriors] = await Promise.all([
    readJsonFile<any>(SCHEDULE_PATH),
    readJsonFile<any>(PASSENGER_PATH),
    readJsonFile<any>(OUTPUT_PATH),
    readJsonFile<any>(HOLIDAY_PATH),
    readJsonFile<any>(MARKET_PRIORS_PATH),
  ]);

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
    advanced: {
      marketPriorsJson: JSON.stringify(marketPriors, null, 2),
    },
    persistenceNote:
      "This editor changes the local repo config files used by the scraper on this machine. Hosted Vercel sessions can view the form, but bundled serverless files are not a reliable long-term place to save operational config.",
  };
}

export async function writeAdminSearchConfig(update: SearchConfigUpdate): Promise<AdminSearchConfig> {
  const [schedule, passenger, output, holidayCalendar] = await Promise.all([
    readJsonFile<any>(SCHEDULE_PATH),
    readJsonFile<any>(PASSENGER_PATH),
    readJsonFile<any>(OUTPUT_PATH),
    readJsonFile<any>(HOLIDAY_PATH),
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

  await Promise.all([
    writeJsonFile(SCHEDULE_PATH, schedule),
    writeJsonFile(PASSENGER_PATH, passenger),
    writeJsonFile(OUTPUT_PATH, output),
    writeJsonFile(HOLIDAY_PATH, holidayCalendar),
    writeJsonFile(MARKET_PRIORS_PATH, marketPriors),
  ]);

  return readAdminSearchConfig();
}
