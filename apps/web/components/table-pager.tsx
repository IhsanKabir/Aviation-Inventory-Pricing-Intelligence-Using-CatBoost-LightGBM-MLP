import Link from "next/link";

import { type PageWindow } from "@/lib/pagination";
import { buildHref, setParam, type RawSearchParams } from "@/lib/query";

const PAGE_WINDOW = 2;

type PageItem = number | "gap";

function buildPageItems(page: number, pageCount: number): PageItem[] {
  const candidates = new Set<number>([1, pageCount]);
  for (let offset = -PAGE_WINDOW; offset <= PAGE_WINDOW; offset += 1) {
    candidates.add(page + offset);
  }
  const pages = [...candidates]
    .filter((item) => item >= 1 && item <= pageCount)
    .sort((left, right) => left - right);

  const items: PageItem[] = [];
  let previous = 0;
  for (const item of pages) {
    if (item - previous > 1) {
      items.push("gap");
    }
    items.push(item);
    previous = item;
  }
  return items;
}

export function TablePager({
  params,
  pageKey,
  pager,
  label,
}: {
  params: RawSearchParams;
  pageKey: string;
  pager: PageWindow;
  label: string;
}) {
  const { page, pageCount, total, rangeStart, rangeEnd } = pager;
  if (pageCount <= 1) {
    return null;
  }

  // Page 1 drops the param entirely so default URLs stay clean; every other
  // param (filters, request_id, load) is preserved by setParam/buildHref.
  const hrefFor = (target: number) =>
    buildHref(setParam(params, pageKey, target === 1 ? undefined : String(target)));

  return (
    <nav className="table-pager" aria-label={`${label} pagination`}>
      <span className="table-pager-info">
        Showing {rangeStart.toLocaleString()}-{rangeEnd.toLocaleString()} of {total.toLocaleString()} {label}
      </span>
      <div className="window-chip-row">
        {page > 1 ? (
          <Link className="window-chip" href={hrefFor(page - 1)} scroll={false}>
            Prev
          </Link>
        ) : (
          <span className="window-chip" data-disabled="true" aria-hidden="true">
            Prev
          </span>
        )}
        {buildPageItems(page, pageCount).map((item, index) =>
          item === "gap" ? (
            <span className="table-pager-gap" key={`gap-${index}`} aria-hidden="true">
              &hellip;
            </span>
          ) : (
            <Link
              key={item}
              className="window-chip"
              data-active={item === page}
              aria-current={item === page ? "page" : undefined}
              href={hrefFor(item)}
              scroll={false}
            >
              {item.toLocaleString()}
            </Link>
          )
        )}
        {page < pageCount ? (
          <Link className="window-chip" href={hrefFor(page + 1)} scroll={false}>
            Next
          </Link>
        ) : (
          <span className="window-chip" data-disabled="true" aria-hidden="true">
            Next
          </span>
        )}
      </div>
    </nav>
  );
}
