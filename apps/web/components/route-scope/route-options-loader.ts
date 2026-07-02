"use client";

import { useDeferredValue, useEffect, useMemo, useState } from "react";

import { getApiBaseUrl } from "@/lib/api";

import type { RouteOption, ScopeState } from "./scope-state";
import { buildRouteOptionsQueryString } from "./scope-state";

export type RouteOptionsState = {
  loading: boolean;
  error?: string;
  data: RouteOption[];
};

const ROUTE_OPTIONS_CACHE_MAX_ENTRIES = 32;
// Module-level cache so route suggestions survive remounts and repeat scope edits
// within the same client session without refetching.
const routeOptionsCache = new Map<string, RouteOption[]>();

function cacheRouteOptions(queryString: string, items: RouteOption[]) {
  routeOptionsCache.set(queryString, items);
  if (routeOptionsCache.size <= ROUTE_OPTIONS_CACHE_MAX_ENTRIES) {
    return;
  }
  const oldestKey = routeOptionsCache.keys().next().value;
  if (typeof oldestKey === "string") {
    routeOptionsCache.delete(oldestKey);
  }
}

export function useRouteOptions(
  initialState: ScopeState,
  initialRouteOptions: RouteOption[],
  state: ScopeState
): RouteOptionsState {
  const initialRouteOptionsQueryString = useMemo(() => buildRouteOptionsQueryString(initialState), [initialState]);
  const [routeOptionsState, setRouteOptionsState] = useState<RouteOptionsState>({
    loading: false,
    data: initialRouteOptions
  });
  const [loadedRouteOptionsQueryString, setLoadedRouteOptionsQueryString] = useState(
    initialRouteOptions.length ? initialRouteOptionsQueryString : ""
  );

  useEffect(() => {
    if (initialRouteOptions.length) {
      cacheRouteOptions(initialRouteOptionsQueryString, initialRouteOptions);
    }
    setRouteOptionsState({
      loading: false,
      data: initialRouteOptions
    });
    setLoadedRouteOptionsQueryString(initialRouteOptions.length ? initialRouteOptionsQueryString : "");
  }, [initialRouteOptionsQueryString, initialRouteOptions]);

  const routeOptionsQueryString = useMemo(() => buildRouteOptionsQueryString(state), [state]);
  const deferredRouteOptionsQueryString = useDeferredValue(routeOptionsQueryString);

  useEffect(() => {
    if (loadedRouteOptionsQueryString === deferredRouteOptionsQueryString && !routeOptionsState.error) {
      return undefined;
    }

    const cachedItems = routeOptionsCache.get(deferredRouteOptionsQueryString);
    if (cachedItems) {
      setRouteOptionsState({
        loading: false,
        error: undefined,
        data: cachedItems
      });
      setLoadedRouteOptionsQueryString(deferredRouteOptionsQueryString);
      return undefined;
    }

    const controller = new AbortController();
    setRouteOptionsState((current) => ({
      ...current,
      loading: true,
      error: undefined
    }));

    const path = deferredRouteOptionsQueryString
      ? `/api/v1/meta/routes?${deferredRouteOptionsQueryString}`
      : "/api/v1/meta/routes";

    fetch(`${getApiBaseUrl()}${path}`, {
      cache: "no-store",
      signal: controller.signal
    })
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(`${response.status} ${response.statusText}`);
        }
        const payload = (await response.json()) as { items?: RouteOption[] };
        const items = Array.isArray(payload.items) ? payload.items : [];
        cacheRouteOptions(deferredRouteOptionsQueryString, items);
        setLoadedRouteOptionsQueryString(deferredRouteOptionsQueryString);
        setRouteOptionsState({
          loading: false,
          error: undefined,
          data: items
        });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) {
          return;
        }
        setRouteOptionsState((current) => ({
          loading: false,
          error: error instanceof Error ? error.message : "Unable to load route suggestions.",
          data: current.data
        }));
      });

    return () => controller.abort();
  }, [deferredRouteOptionsQueryString, loadedRouteOptionsQueryString, routeOptionsState.error]);

  return routeOptionsState;
}
