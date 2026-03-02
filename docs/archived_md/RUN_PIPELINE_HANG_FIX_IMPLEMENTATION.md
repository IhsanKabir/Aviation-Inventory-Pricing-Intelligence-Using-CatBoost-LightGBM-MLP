# Quick Implementation Guide: Fix the Hang (40 Minutes)

This guide shows **exact code changes** to fix the pipeline hang. Complete in 40 minutes!

---

## Change #1: Fix ThreadPoolExecutor (5 minutes)

### File: `run_all.py`

**FIND** (around line 15, in the imports section):
```python
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
```

**ADD AFTER** all imports (around line 28):
```python
# Module-level executor pool (reused across all queries)
_EXECUTOR = ThreadPoolExecutor(max_workers=5, thread_name_prefix="RunAll_Query_")
```

---

**FIND** (around lines 763-779):
```python
def _call_with_timeout(fn, timeout_seconds: float, *args, **kwargs):
    if timeout_seconds is None or timeout_seconds <= 0:
        return fn(*args, **kwargs)
    ex = ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(fn, *args, **kwargs)
    try:
        return fut.result(timeout=float(timeout_seconds))
    except FutureTimeoutError:
        fut.cancel()
        ex.shutdown(wait=False, cancel_futures=True)
        raise
    except Exception:
        ex.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        if fut.done():
            ex.shutdown(wait=False, cancel_futures=True)
```

**REPLACE WITH**:
```python
def _call_with_timeout(fn, timeout_seconds: float, *args, **kwargs):
    if timeout_seconds is None or timeout_seconds <= 0:
        return fn(*args, **kwargs)
    fut = _EXECUTOR.submit(fn, *args, **kwargs)
    try:
        return fut.result(timeout=float(timeout_seconds))
    except FutureTimeoutError:
        fut.cancel()
        raise
    except Exception:
        raise
```

---

**FIND** (at the very end of `main()` function, around line 1680, just before the final `except KeyboardInterrupt:`):
```python
    LOG.info("Runtime profile written: %s", latest)

if __name__ == "__main__":
```

**ADD BEFORE** the `if __name__` line:
```python
    
    # Cleanup executor pool
    try:
        _EXECUTOR.shutdown(wait=True, timeout=10)
    except Exception:
        pass
```

**Result:** Should look like:
```python
    LOG.info("Runtime profile written: %s", latest)
    
    # Cleanup executor pool
    try:
        _EXECUTOR.shutdown(wait=True, timeout=10)
    except Exception:
        pass

if __name__ == "__main__":
```

---

## Change #2: Fix DB Connection Pool (2 minutes)

### File: `db.py`

**FIND** (around line 22):
```python
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
```

**REPLACE WITH**:
```python
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True, 
    future=True,
    pool_size=30,          # Increased from default 5
    max_overflow=50,       # Increased from default 10
    pool_recycle=3600,     # Recycle connections every hour (prevent stale connections)
    pool_timeout=30        # Timeout after 30s waiting for a connection
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
```

---

## Change #3: Add Query Timeout (3 minutes)

### File: `run_all.py`

**FIND** (around line 430, in `preload_previous_snapshots()` function):
```python
def preload_previous_snapshots(
    *,
    session,
    current_scrape_id,
    airline: str,
    origin: str,
    destination: str,
    cabin: str,
    departure_days: list[str],
):
    parsed_days = []
```

**ADD AFTER the function definition** (before `parsed_days = []`):
```python
    # Set query timeout to prevent hung queries
    session.execute(text("SET statement_timeout = '30s'"))
```

**Result should look like:**
```python
def preload_previous_snapshots(
    *,
    session,
    current_scrape_id,
    airline: str,
    origin: str,
    destination: str,
    cabin: str,
    departure_days: list[str],
):
    # Set query timeout to prevent hung queries
    session.execute(text("SET statement_timeout = '30s'"))
    
    parsed_days = []
```

---

## Change #4: Optimize Preload Query (30 minutes - OPTIONAL but RECOMMENDED)

### File: `run_all.py`

This is the slowest query. **Only do this if you have time** - Changes #1 and #2 above will already fix most hangs.

**FIND** (around line 434, the SQL query in `preload_previous_snapshots()`):
```python
    sql = text(
        """
        WITH ranked AS (
            SELECT
                fo.airline,
                fo.origin,
                fo.destination,
                fo.departure,
                fo.flight_number,
                fo.cabin,
                fo.fare_basis,
                fo.brand,
                fo.price_total_bdt,
                fo.seat_available,
                fo.seat_capacity,
                fo.scraped_at,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        fo.airline,
                        fo.origin,
                        fo.destination,
                        fo.departure,
                        fo.flight_number,
                        fo.cabin,
                        COALESCE(fo.fare_basis, ''),
                        COALESCE(fo.brand, '')
                    ORDER BY fo.scraped_at DESC, fo.id DESC
                ) AS rn
            FROM flight_offers fo
            WHERE fo.airline = :airline
              AND fo.origin = :origin
              AND fo.destination = :destination
              AND fo.cabin = :cabin
              AND fo.scrape_id <> :current_scrape_id
              AND fo.departure >= :min_day
              AND fo.departure < :max_day
        )
        SELECT
            airline,
            origin,
            destination,
            departure,
            flight_number,
            cabin,
            fare_basis,
            brand,
            price_total_bdt,
            seat_available,
            seat_capacity,
            scraped_at
        FROM ranked
        WHERE rn = 1
        """
    )
```

**REPLACE WITH** (PostgreSQL DISTINCT ON is faster):
```python
    sql = text(
        """
        SELECT DISTINCT ON (
            airline, origin, destination, departure, 
            flight_number, cabin, COALESCE(fare_basis, ''), COALESCE(brand, '')
        )
            airline,
            origin,
            destination,
            departure,
            flight_number,
            cabin,
            fare_basis,
            brand,
            price_total_bdt,
            seat_available,
            seat_capacity,
            scraped_at
        FROM flight_offers
        WHERE airline = :airline
          AND origin = :origin
          AND destination = :destination
          AND cabin = :cabin
          AND scrape_id <> :current_scrape_id
          AND departure >= :min_day
          AND departure < :max_day
        ORDER BY 
            airline, origin, destination, departure, 
            flight_number, cabin, COALESCE(fare_basis, ''), COALESCE(brand, ''),
            scraped_at DESC, id DESC
        """
    )
```

---

## Verification Checklist

After making changes, verify each one:

### ✅ Change #1 Verification
```bash
# Search for "ThreadPoolExecutor(" in run_all.py
grep -n "ThreadPoolExecutor(" run_all.py
# Should show ONLY the _EXECUTOR definition, NOT inside _call_with_timeout()
```

### ✅ Change #2 Verification
```python
# In Python, check the engine settings
from db import engine
print(engine.pool)
# Should show pool_size=30, max_overflow=50
```

### ✅ Change #3 Verification
- Run `python run_all.py --quick --limit-routes 2`
- Should see in logs: "SET statement_timeout" or start without error

### ✅ Change #4 Verification (if done)
```bash
# Test the query directly in psql
psql $AIRLINE_DB_URL -c "EXPLAIN ANALYZE SELECT DISTINCT ON (...) FROM flight_offers WHERE ..."
# Should take < 1 second
```

---

## Testing After Changes

### Test 1: Quick Run (2-3 minutes)
```bash
python run_all.py --quick --limit-routes 2
# Should complete in ~1-3 minutes
```

### Test 2: Medium Run (10-15 minutes)
```bash
python run_all.py --quick --limit-routes 10
# Should complete in ~10-15 minutes
```

### Test 3: Full Run (Overnight)
```bash
nohup python run_all.py > run_all.log 2>&1 &
tail -f run_all.log
# Monitor for 2+ hours to verify no hang
```

---

## Success Indicators

After applying fixes, you should see:

✅ **Logs show consistent progress** (no stalling)
```
[12:00] Query 1/100
[12:01] Query 2/100
[12:02] Query 3/100
...
```

✅ **Query times stable** (not increasing over time)
```
Progress: 10/100 queries done | last=1.23s avg=1.20s
Progress: 20/100 queries done | last=1.25s avg=1.22s
Progress: 30/100 queries done | last=1.19s avg=1.21s
```

✅ **DB connections in use** (not maxed out)
```
SELECT count(*) FROM pg_stat_activity;
# Should be < 20 (not > 30)
```

✅ **Thread count stable** (not growing)
```
ps -eLf | grep run_all | wc -l
# Should be ~10-15 threads (not > 50)
```

---

## Rollback (If Something Breaks)

If a change causes issues, revert it:

### Rollback Change #1
Comment out or delete the `_EXECUTOR` lines and put the old code back

### Rollback Change #2
Revert `db.py` line 22 to original: `engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)`

### Rollback Change #3
Remove the `session.execute(text("SET statement_timeout = '30s'"))` line

### Rollback Change #4
Revert the SQL query to the original WITH/ROW_NUMBER version

---

## Expected Performance Improvement

After applying **all 4 changes**:

| Metric | Before | After |
|--------|--------|-------|
| 100 routes, 8 dates | Hangs @ 2-3 hours | Completes in 30-40 min |
| 1000 routes, 8 dates | Hangs @ 4-6 hours | Completes in 2-3 hours |
| Queries per minute | 5-10 | 20-30 |
| Database pool utilization | 100% (stuck) | 20-30% (healthy) |
| Thread count growth | Unbounded (50+) | Stable (5-10) |

---

## If Issues Remain

If the pipeline still hangs after these changes, check:

1. **Database logs:**
   ```bash
   tail -100 /var/log/postgresql/postgresql.log
   # Look for connection timeouts, locks, deadlocks
   ```

2. **System resources:**
   ```bash
   # Monitor during a run
   top
   vmstat 1
   iostat -x 1
   ```

3. **Long-running queries:**
   ```sql
   SELECT pid, now() - pg_stat_activity.query_start AS duration, query
   FROM pg_stat_activity
   WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';
   ```

4. **Connection state:**
   ```sql
   SELECT * FROM pg_stat_activity WHERE state != 'idle';
   ```

---

## Questions?

If you get stuck on any change, check the RUN_PIPELINE_HANG_ANALYSIS_AND_FIXES.md document for more detailed explanations.

**Expected time to complete: 40 minutes (min fixes) to 75 minutes (all fixes)**
