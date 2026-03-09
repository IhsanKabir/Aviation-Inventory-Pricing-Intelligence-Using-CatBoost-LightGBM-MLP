export function formatDhakaDateTime(value?: string | null) {
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Asia/Dhaka"
  }).format(new Date(value));
}

export function formatDhakaDate(value?: string | null) {
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeZone: "Asia/Dhaka"
  }).format(new Date(value));
}

export function formatMoney(value?: number | null, currency = "BDT") {
  if (value === null || value === undefined) {
    return "-";
  }
  const hasDecimals = Math.abs(value % 1) > Number.EPSILON;
  return `${currency} ${value.toLocaleString("en-US", {
    maximumFractionDigits: hasDecimals ? 2 : 0,
    minimumFractionDigits: hasDecimals ? 2 : 0
  })}`;
}

export function formatNumber(value?: number | null) {
  if (value === null || value === undefined) {
    return "-";
  }
  return value.toLocaleString("en-US");
}

export function formatPercent(value?: number | null) {
  if (value === null || value === undefined) {
    return "-";
  }
  return `${value.toFixed(1)}%`;
}

export function formatBooleanFlag(value?: boolean | null, trueLabel = "Yes", falseLabel = "No") {
  if (value === null || value === undefined) {
    return "-";
  }
  return value ? trueLabel : falseLabel;
}

export function shortCycle(value?: string | null) {
  return value ? value.slice(0, 8) : "None";
}

export function formatPublicBrand(value?: string | null) {
  if (!value) {
    return "-";
  }

  const normalized = value.trim();
  if (!normalized) {
    return "-";
  }

  if (
    /_OTA$/i.test(normalized) ||
    /unknown_ota/i.test(normalized) ||
    /sharetrip|amybd|bdfare|gozayaan|akijair/i.test(normalized)
  ) {
    return "OTA fare";
  }

  return normalized;
}

export function formatPublicValue(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }

  if (typeof value === "string") {
    const normalized = value.trim();
    if (!normalized) {
      return "-";
    }

    if (
      /unknown_ota/i.test(normalized) ||
      /sharetrip|amybd|bdfare|gozayaan|akijair/i.test(normalized) ||
      /_OTA$/i.test(normalized)
    ) {
      return "OTA fare";
    }

    return normalized;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  try {
    const encoded = JSON.stringify(value);
    if (
      /unknown_ota/i.test(encoded) ||
      /sharetrip|amybd|bdfare|gozayaan|akijair/i.test(encoded) ||
      /_OTA"/i.test(encoded)
    ) {
      return "OTA fare";
    }
    return encoded.length > 120 ? `${encoded.slice(0, 117)}...` : encoded;
  } catch {
    return "[unserializable]";
  }
}

export function summarizePenaltyText(value?: string | null) {
  if (!value) {
    return "-";
  }

  const lines = value
    .replace(/\r/g, "")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line && !/^[-/]{3,}$/.test(line));

  if (!lines.length) {
    return "-";
  }

  const preview = lines.slice(0, 4).join(" | ");
  return preview.length > 220 ? `${preview.slice(0, 217)}...` : preview;
}

export function normalizeLongText(value?: string | null) {
  if (!value) {
    return "-";
  }

  const normalized = value
    .replace(/\r/g, "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .join("\n");

  return normalized || "-";
}

export function formatRouteType(routeType?: string | null) {
  if (routeType === "DOM" || routeType === "INT") {
    return routeType;
  }
  return "UNK";
}

export function formatRouteGeo(originCountryCode?: string | null, destinationCountryCode?: string | null) {
  if (originCountryCode && destinationCountryCode) {
    return `${originCountryCode} -> ${destinationCountryCode}`;
  }
  if (originCountryCode) {
    return `${originCountryCode} -> ?`;
  }
  if (destinationCountryCode) {
    return `? -> ${destinationCountryCode}`;
  }
  return "Country map pending";
}

export function formatRouteTypeDetail(
  routeType?: string | null,
  originCountryCode?: string | null,
  destinationCountryCode?: string | null
) {
  if (routeType === "DOM" && originCountryCode) {
    return `Domestic within ${originCountryCode}`;
  }
  if (routeType === "INT") {
    return formatRouteGeo(originCountryCode, destinationCountryCode);
  }
  return `Unknown scope · ${formatRouteGeo(originCountryCode, destinationCountryCode)}`;
}
