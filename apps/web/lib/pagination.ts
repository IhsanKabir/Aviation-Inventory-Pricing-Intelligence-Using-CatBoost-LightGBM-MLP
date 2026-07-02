export const DEFAULT_PAGE_SIZE = 50;

export interface PageWindow {
  page: number;
  pageCount: number;
  total: number;
  rangeStart: number;
  rangeEnd: number;
}

export interface PaginatedRows<T> extends PageWindow {
  pageRows: T[];
}

export function parsePageParam(value: string | undefined): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return 1;
  }
  return parsed;
}

export function paginateRows<T>(
  rows: readonly T[],
  requestedPage: number,
  pageSize = DEFAULT_PAGE_SIZE
): PaginatedRows<T> {
  const total = rows.length;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  // Clamp instead of erroring so stale ?page= values (e.g. after a filter
  // change shrinks the result set) still render the closest valid page.
  const page = Math.min(Math.max(1, requestedPage), pageCount);
  const startIndex = (page - 1) * pageSize;
  const pageRows = rows.slice(startIndex, startIndex + pageSize);
  return {
    pageRows,
    page,
    pageCount,
    total,
    rangeStart: total === 0 ? 0 : startIndex + 1,
    rangeEnd: startIndex + pageRows.length,
  };
}
