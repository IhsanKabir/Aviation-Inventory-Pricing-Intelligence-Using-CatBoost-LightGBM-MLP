"""
Robust parser for the airline response.
This implementation is defensive and returns a list of dicts.
If response structure differs, it returns an empty list rather than throwing.
Signature: parse_response(data, equip_map=None)
"""

def parse_response(data, equip_map=None):
    rows = []
    if not data:
        return rows
    # Example GraphQL shape we expect:
    # data['data']['searchFlights']['results'] -> list
    payload = data.get("data") or data
    # Try several common nested paths
    candidates = []
    # common GraphQL wrapper
    if isinstance(payload, dict):
        for key in ("searchFlights", "availability", "flightSearch", "getAvailability"):
            if key in payload:
                try:
                    candidates.append(payload[key])
                except Exception:
                    pass
    # fallback: top-level 'results' or 'flights'
    if isinstance(payload, dict):
        for key in ("results", "flights", "itineraries"):
            if key in payload:
                candidates.append(payload[key])

    # If candidates empty, attempt to find lists deeper in payload
    if not candidates:
        # naive search for the first list of dicts
        def find_first_list_of_dicts(obj):
            if isinstance(obj, list):
                if obj and isinstance(obj[0], dict):
                    return obj
            if isinstance(obj, dict):
                for v in obj.values():
                    res = find_first_list_of_dicts(v)
                    if res:
                        return res
            return None
        found = find_first_list_of_dicts(payload)
        if found:
            candidates.append(found)

    # Flatten candidate lists into rows
    for c in candidates:
        if isinstance(c, dict):
            # inside c might be {'results': [...]}
            inner = None
            for k in ("results", "flights", "itineraries", "offers"):
                if k in c and isinstance(c[k], list):
                    inner = c[k]
                    break
            if inner:
                for item in inner:
                    rows.append(_normalize_item(item))
            else:
                # treat dict itself as single row
                rows.append(_normalize_item(c))
        elif isinstance(c, list):
            for item in c:
                rows.append(_normalize_item(item))
    # final cleanup: ensure list of dicts, otherwise return []
    cleaned = [r for r in rows if isinstance(r, dict)]
    return cleaned

def _normalize_item(item):
    """
    Convert different possible flight item shapes into a flat dict.
    This is intentionally conservative: we extract common keys if present.
    """
    out = {}
    if not isinstance(item, dict):
        return out
    # Pull obvious fields if present
    for k in ("flightNumber", "flight_number", "flight", "number"):
        v = item.get(k)
        if v:
            out["flight_number"] = v
            break
    for k in ("price", "totalPrice", "fare", "amount"):
        v = item.get(k)
        if v:
            out["price"] = v
            break
    for k in ("currency",):
        v = item.get(k)
        if v:
            out["currency"] = v
            break
    # origin / destination may be nested
    o = item.get("origin") or item.get("departure") or item.get("from")
    d = item.get("destination") or item.get("arrival") or item.get("to")
    if isinstance(o, dict):
        out["origin"] = o.get("code") or o.get("iata") or o.get("location")
    elif isinstance(o, str):
        out["origin"] = o
    if isinstance(d, dict):
        out["destination"] = d.get("code") or d.get("iata") or d.get("location")
    elif isinstance(d, str):
        out["destination"] = d
    # durations / stops / cabin
    for k in ("duration", "travel_time"):
        if k in item:
            out["duration"] = item[k]
            break
    if "stops" in item:
        out["stops"] = item["stops"]
    # fallback: copy some keys directly if they exist (avoid overwriting above)
    for k in ("airline", "equipment", "cabinClass", "class"):
        if k in item and k not in out:
            out[k.lower()] = item[k]
    return out
