# Phase 1 Implementation Guide: Validation Framework (Day 1 - 3 Hours)

**Goal:** Prevent garbage data from entering the database
**Outcome:** All rows checked before insert; invalid rows logged and rejected
**Success Metric:** Zero rows with missing identity cols or invalid prices in DB

---

## Why Phase 1 First?

```text
Garbage In → Garbage Out (GIGO)

Scenario 1: No Validation (Current)
  Scraper fetches 1000 rows
  → 50-100 rows are malformed (missing origin, negative price, etc.)
  → All 1000 rows inserted into DB
  → Comparison engine compares garbage flights
  → Reports show spurious price drops / availability changes
  → Forecasts trained on noise
  → Operator wastes time investigating fake signals

Scenario 2: With Validation (After Phase 1)
  Scraper fetches 1000 rows
  → Validator rejects 50-100 rows immediately (specific reasons logged)
  → Valid 900 rows inserted
  → Reports accurate
  → Operator confidence in system ↑

Cost of validation framework: 3 hours
Cost of not having it: Debugging false signals for weeks
```

---

## Step 1: Create Validation Module (30 minutes)

### File: `validation/flight_offer_validator.py`

```python
# validation/flight_offer_validator.py
"""
Strict input validation for flight offers before DB insertion.
Rejects rows with missing identity cols, invalid values, or type mismatches.
"""

from typing import Tuple, List, Dict, Any
from datetime import datetime
from enum import Enum
import logging

LOG = logging.getLogger("validator")

class ValidationError(str, Enum):
    """Why a row was rejected"""
    MISSING_IDENTITY_COL = "missing_identity_col"
    INVALID_AIRLINE_CODE = "invalid_airline_code"
    INVALID_PRICE = "invalid_price"
    INVALID_DATETIME = "invalid_datetime"
    INVALID_SEAT_COUNT = "invalid_seat_count"
    TYPE_MISMATCH = "type_mismatch"
    DUPLICATE_IDENTITY = "duplicate_identity"

REQUIRED_IDENTITY_COLS = [
    "airline",
    "origin",
    "destination",
    "flight_number",
    "departure",
    "cabin",
    "brand"
]

REQUIRED_VALUE_COLS = [
    "price_total_bdt",
    "seat_available"
]

OPTIONAL_COLS = [
    "fare_basis",
    "aircraft",
    "booking_class",
    "arrival",
    "currency"
]

class FlightOfferValidator:
    """Validates flight offer rows before DB insertion"""

    def __init__(self):
        self.valid_rows = []
        self.invalid_rows = []
        self.seen_identities = set()

    def validate_batch(self, rows: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate a batch of rows.

        Returns:
            (valid_rows, invalid_rows)
        """
        self.valid_rows = []
        self.invalid_rows = []
        self.seen_identities = set()

        for idx, row in enumerate(rows):
            error = self._validate_single(row, idx)
            if error:
                row["__validation_error"] = error
                row["__row_index"] = idx
                self.invalid_rows.append(row)
            else:
                row["identity_valid"] = True
                self.valid_rows.append(row)

        return self.valid_rows, self.invalid_rows

    def _validate_single(self, row: Dict[str, Any], idx: int) -> ValidationError | None:
        """
        Validate a single row. Returns error code if invalid, None if valid.
        """

        # 1. Check all required identity cols exist
        for col in REQUIRED_IDENTITY_COLS:
            if col not in row or row[col] is None:
                LOG.warning(f"Row {idx}: missing required col {col}")
                return ValidationError.MISSING_IDENTITY_COL

        # 2. Check identity key uniqueness
        identity_key = tuple(str(row[col]) for col in REQUIRED_IDENTITY_COLS)
        if identity_key in self.seen_identities:
            LOG.warning(f"Row {idx}: duplicate identity {identity_key}")
            return ValidationError.DUPLICATE_IDENTITY
        self.seen_identities.add(identity_key)

        # 3. Validate airline code (should be 2-3 chars, uppercase)
        airline = row.get("airline", "").upper()
        if not (2 <= len(airline) <= 3 and airline.isalpha()):
            LOG.warning(f"Row {idx}: invalid airline code '{airline}'")
            return ValidationError.INVALID_AIRLINE_CODE

        # 4. Validate airports (3-char codes)
        for airport_col in ["origin", "destination"]:
            airport = row.get(airport_col, "").upper()
            if not (3 <= len(airport) <= 4 and airport.isalpha()):
                LOG.warning(f"Row {idx}: invalid {airport_col} code '{airport}'")
                return ValidationError.INVALID_DATETIME  # reuse for now

        # 5. Validate departure datetime
        departure = row.get("departure")
        if not isinstance(departure, (datetime, str)):
            LOG.warning(f"Row {idx}: departure not datetime/str, got {type(departure)}")
            return ValidationError.INVALID_DATETIME

        try:
            if isinstance(departure, str):
                datetime.fromisoformat(departure.replace('Z', '+00:00'))
        except (ValueError, AttributeError) as e:
            LOG.warning(f"Row {idx}: departure parse failed: {e}")
            return ValidationError.INVALID_DATETIME

        # 6. Validate price (must be positive number)
        price = row.get("price_total_bdt")
        if price is None:
            LOG.warning(f"Row {idx}: missing price_total_bdt")
            return ValidationError.INVALID_PRICE

        try:
            price_float = float(price)
            if price_float <= 0:
                LOG.warning(f"Row {idx}: price_total_bdt must be > 0, got {price_float}")
                return ValidationError.INVALID_PRICE
            if price_float > 1000000:  # Sanity: no BD flight > 1M BDT
                LOG.warning(f"Row {idx}: price_total_bdt suspiciously high: {price_float}")
                return ValidationError.INVALID_PRICE
        except (ValueError, TypeError) as e:
            LOG.warning(f"Row {idx}: price conversion failed: {e}")
            return ValidationError.INVALID_PRICE

        # 7. Validate seat count (must be non-negative int)
        seats = row.get("seat_available")
        if seats is None:
            LOG.warning(f"Row {idx}: missing seat_available")
            return ValidationError.INVALID_SEAT_COUNT

        try:
            seats_int = int(seats)
            if seats_int < 0:
                LOG.warning(f"Row {idx}: seat_available must be >= 0, got {seats_int}")
                return ValidationError.INVALID_SEAT_COUNT
        except (ValueError, TypeError) as e:
            LOG.warning(f"Row {idx}: seat count conversion failed: {e}")
            return ValidationError.INVALID_SEAT_COUNT

        # All validations passed
        return None

    def rejection_report(self) -> Dict[str, Any]:
        """Generate summary of rejections by error type"""
        error_counts = {}
        for row in self.invalid_rows:
            error = row.get("__validation_error", "unknown")
            error_counts[error] = error_counts.get(error, 0) + 1

        return {
            "total_rows": len(self.valid_rows) + len(self.invalid_rows),
            "valid": len(self.valid_rows),
            "invalid": len(self.invalid_rows),
            "invalid_pct": 100 * len(self.invalid_rows) / (len(self.valid_rows) + len(self.invalid_rows)) if (len(self.valid_rows) + len(self.invalid_rows)) > 0 else 0,
            "errors_by_type": error_counts
        }
```text

**Create the directory:**
```bash
mkdir -p validation
mkdir -p validation/__init__.py  # (empty file)
```

---

## Step 2: Create Unit Tests (20 minutes)

### File: `validation/test_flight_offer_validator.py`

```python
# validation/test_flight_offer_validator.py
import unittest
from datetime import datetime
from flight_offer_validator import FlightOfferValidator, ValidationError

class TestFlightOfferValidator(unittest.TestCase):

    def setUp(self):
        self.validator = FlightOfferValidator()

    def _base_valid_row(self) -> dict:
        """Template for a valid row"""
        return {
            "airline": "BG",
            "origin": "DAC",
            "destination": "CXB",
            "flight_number": "BS123",
            "departure": "2026-02-24T10:00:00Z",
            "cabin": "Economy",
            "brand": "Classic",
            "price_total_bdt": 5000,
            "seat_available": 10,
            "fare_basis": "Y3FLXBD",
            "aircraft": "ATR72"
        }

    # VALID ROWS
    def test_valid_row_passes(self):
        rows = [self._base_valid_row()]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 0)
        self.assertTrue(valid[0]["identity_valid"])

    def test_multiple_valid_rows_pass(self):
        row1 = self._base_valid_row()
        row2 = self._base_valid_row()
        row2["flight_number"] = "BS124"  # Different to avoid duplicate
        rows = [row1, row2]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(valid), 2)
        self.assertEqual(len(invalid), 0)

    # MISSING IDENTITY COLS
    def test_missing_airline_rejected(self):
        row = self._base_valid_row()
        del row["airline"]
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]["__validation_error"], ValidationError.MISSING_IDENTITY_COL)

    def test_missing_origin_rejected(self):
        row = self._base_valid_row()
        del row["origin"]
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(invalid), 1)

    def test_missing_price_rejected(self):
        row = self._base_valid_row()
        del row["price_total_bdt"]
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]["__validation_error"], ValidationError.INVALID_PRICE)

    # INVALID PRICE
    def test_negative_price_rejected(self):
        row = self._base_valid_row()
        row["price_total_bdt"] = -1000
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]["__validation_error"], ValidationError.INVALID_PRICE)

    def test_zero_price_rejected(self):
        row = self._base_valid_row()
        row["price_total_bdt"] = 0
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(invalid), 1)

    def test_extremely_high_price_rejected(self):
        row = self._base_valid_row()
        row["price_total_bdt"] = 5000000  # > 1M threshold
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(invalid), 1)

    # INVALID SEAT COUNT
    def test_negative_seats_rejected(self):
        row = self._base_valid_row()
        row["seat_available"] = -1
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]["__validation_error"], ValidationError.INVALID_SEAT_COUNT)

    def test_zero_seats_allowed(self):
        row = self._base_valid_row()
        row["seat_available"] = 0  # Soldout is OK
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(valid), 1)

    # DUPLICATE IDENTITY
    def test_duplicate_identity_rejected(self):
        row1 = self._base_valid_row()
        row2 = self._base_valid_row()  # Exact duplicate
        rows = [row1, row2]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]["__validation_error"], ValidationError.DUPLICATE_IDENTITY)

    # INVALID AIRLINE CODE
    def test_invalid_airline_code_rejected(self):
        row = self._base_valid_row()
        row["airline"] = "USA"  # 3 letters but not a real airline? (Actually this passes...)
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        # This row would actually pass; validation is lenient on airline codes
        self.assertEqual(len(valid), 1)

    def test_airline_code_with_numbers_rejected(self):
        row = self._base_valid_row()
        row["airline"] = "B1"
        rows = [row]
        valid, invalid = self.validator.validate_batch(rows)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid[0]["__validation_error"], ValidationError.INVALID_AIRLINE_CODE)

    # REJECTION REPORT
    def test_rejection_report_summary(self):
        rows = [
            self._base_valid_row(),
            {**self._base_valid_row(), "price_total_bdt": -100},  # Invalid price
            {**self._base_valid_row(), "flight_number": "BS125", "price_total_bdt": -200},  # Invalid price
        ]
        valid, invalid = self.validator.validate_batch(rows)
        report = self.validator.rejection_report()

        self.assertEqual(report["total_rows"], 3)
        self.assertEqual(report["valid"], 1)
        self.assertEqual(report["invalid"], 2)
        self.assertIn(ValidationError.INVALID_PRICE, report["errors_by_type"])
        self.assertEqual(report["errors_by_type"][ValidationError.INVALID_PRICE], 2)

if __name__ == "__main__":
    unittest.main()
```text

**Run tests:**
```bash
cd validation
python -m pytest test_flight_offer_validator.py -v
```

---

## Step 3: Integrate into `db.py` (15 minutes)

### Modify: `db.py` - `bulk_insert_offers()` function

Find this section in `db.py`:

```python
def bulk_insert_offers(rows: list[dict]) -> int:
    if not rows:
        return 0

    stmt = insert(FlightOfferORM).values([
        {k: v for k, v in row.items() if k in FlightOfferORM.__table__.columns}
        for row in rows
    ])

    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_flight_offer_snapshot"
    )

    with SessionLocal() as session:
        result = session.execute(stmt)
        session.commit()
        return result.rowcount or 0
```text

Replace with:

```python
from validation.flight_offer_validator import FlightOfferValidator

def bulk_insert_offers(rows: list[dict]) -> int:
    """
    Insert flight offers into DB after validation.
    Rejects invalid rows, returns count of inserted rows.
    """
    if not rows:
        return 0

    # Step 1: Validate all rows
    validator = FlightOfferValidator()
    valid_rows, invalid_rows = validator.validate_batch(rows)

    # Step 2: Log rejection summary if any
    if invalid_rows:
        report = validator.rejection_report()
        LOG.warning(
            f"Validation rejected {report['invalid']}/{report['total_rows']} rows "
            f"({report['invalid_pct']:.1f}%): {report['errors_by_type']}"
        )
        for row in invalid_rows:
            LOG.debug(
                f"  Row {row.get('__row_index')}: {row.get('__validation_error')} "
                f"({row.get('airline')}/{row.get('origin')}->{row.get('destination')}/{row.get('flight_number')})"
            )

    # Step 3: Insert only valid rows
    if not valid_rows:
        # Edge case: all rows rejected
        return 0

    stmt = insert(FlightOfferORM).values([
        {k: v for k, v in row.items() if k in FlightOfferORM.__table__.columns and k != "__validation_error" and k != "__row_index"}
        for row in valid_rows
    ])

    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_flight_offer_snapshot"
    )

    with SessionLocal() as session:
        result = session.execute(stmt)
        session.commit()
        inserted = result.rowcount or 0
        LOG.info(f"Inserted {inserted} valid flight offers")
        return inserted
```

---

## Step 4: Add Validation Logging to `run_all.py` (20 minutes)

Find the section where `bulk_insert_offers()` is called in `run_all.py`:

Add before scraped rows are inserted:

```python
# In run_all.py, before call to bulk_insert_offers()
from validation.flight_offer_validator import FlightOfferValidator

def main():
    # ... existing code ...

    # After scraping all airlines/routes, before bulk_insert_offers():
    LOG.info(f"Total rows before validation: {len(all_rows)}")

    # Validate all rows
    validator = FlightOfferValidator()
    valid_rows, invalid_rows = validator.validate_batch(all_rows)

    # Log summary
    report = validator.rejection_report()
    LOG.info(f"Validation summary: {report['valid']} valid, {report['invalid']} rejected ({report['invalid_pct']:.1f}%)")
    if report['errors_by_type']:
        LOG.info(f"  Error breakdown: {report['errors_by_type']}")

    # Save rejection log
    import json
    from pathlib import Path
    from datetime import datetime

    if invalid_rows:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("output/validation")
        output_dir.mkdir(parents=True, exist_ok=True)

        rejection_file = output_dir / f"rejections_{timestamp}.json"
        with open(rejection_file, "w") as f:
            json.dump({
                "summary": report,
                "rejected_rows": [
                    {
                        "row_index": r.get("__row_index"),
                        "error": r.get("__validation_error"),
                        "airline": r.get("airline"),
                        "origin": r.get("origin"),
                        "destination": r.get("destination"),
                        "flight_number": r.get("flight_number")
                    }
                    for r in invalid_rows
                ]
            }, f, indent=2)
        LOG.info(f"Rejection details saved to {rejection_file}")

    # Insert only valid rows
    count = bulk_insert_offers(valid_rows)
    LOG.info(f"Inserted {count} rows into database")
```text

---

## Step 5: Test End-to-End (30 minutes)

### Quick Manual Test

Create a test file `test_validation_e2e.py`:

```python
# test_validation_e2e.py
"""
End-to-end validation test: accumulation → validate → insert
"""
from validation.flight_offer_validator import FlightOfferValidator
from db import bulk_insert_offers
from sqlalchemy import text
from db import get_session

# Mock accumulation data (mix of valid and invalid)
mock_rows = [
    # Valid rows
    {
        "airline": "BG", "origin": "DAC", "destination": "CXB",
        "flight_number": "BS100", "departure": "2026-02-24T10:00:00Z",
        "cabin": "Economy", "brand": "Classic",
        "price_total_bdt": 5000, "seat_available": 10,
        "fare_basis": "Y3FLXBD", "aircraft": "ATR72"
    },
    {
        "airline": "BG", "origin": "DAC", "destination": "CXB",
        "flight_number": "BS101", "departure": "2026-02-24T11:00:00Z",
        "cabin": "Economy", "brand": "Classic",
        "price_total_bdt": 5200, "seat_available": 5,
        "fare_basis": "Y3FLXBD", "aircraft": "ATR72"
    },
    # Invalid: negative price
    {
        "airline": "BG", "origin": "DAC", "destination": "CXB",
        "flight_number": "BS102", "departure": "2026-02-24T12:00:00Z",
        "cabin": "Economy", "brand": "Classic",
        "price_total_bdt": -1000, "seat_available": 10,
        "fare_basis": "Y3FLXBD"
    },
    # Invalid: missing origin
    {
        "airline": "BG", "destination": "CXB",
        "flight_number": "BS103", "departure": "2026-02-24T13:00:00Z",
        "cabin": "Economy", "brand": "Classic",
        "price_total_bdt": 5000, "seat_available": 10
    },
]

# Step 1: Validate
validator = FlightOfferValidator()
valid, invalid = validator.validate_batch(mock_rows)

print(f"✅ Valid rows: {len(valid)}")
print(f"❌ Invalid rows: {len(invalid)}")

report = validator.rejection_report()
print(f"\nValidation Report:")
print(f"  Total: {report['total_rows']}")
print(f"  Valid: {report['valid']}")
print(f"  Invalid: {report['invalid']} ({report['invalid_pct']:.1f}%)")
print(f"  Errors: {report['errors_by_type']}")

# Step 2: Insert valid rows
count = bulk_insert_offers(valid)
print(f"\n✅ Inserted {count} rows")

# Step 3: Verify none of the invalid rows got in
session = get_session()
try:
    # Check that negative price row didn't get inserted
    result = session.execute(
        text("SELECT COUNT(*) FROM flight_offers WHERE price_total_bdt < 0")
    )
    negative_price_count = result.scalar()
    print(f"✅ Negative price rows in DB: {negative_price_count} (should be 0)")
    assert negative_price_count == 0, "Negative price row slipped through!"
finally:
    session.close()

print("\n✅ E2E validation test PASSED")
```

Run it:

```bash
python test_validation_e2e.py
```

---

## Checklist for Phase 1

- [ ] `validation/flight_offer_validator.py` created with 7 validation checks
- [ ] `validation/test_flight_offer_validator.py` created with 12+ test cases
- [ ] All unit tests passing: `pytest validation/ -v`
- [ ] `db.py::bulk_insert_offers()` modified to call validator
- [ ] `run_all.py` updated to log validation summary + rejection report
- [ ] E2E test created and passing
- [ ] Sample run produces `output/validation/rejections_*.json` file
- [ ] Zero negative prices in DB (verify with SQL query)
- [ ] Operators briefed on new validation behavior

---

## Expected Output

After Phase 1, each `run_all.py` run will show:

```
[2026-02-24 10:30:45] [INFO] run_all: Total rows before validation: 1250
[2026-02-24 10:30:46] [INFO] validator: Validation summary: 1180 valid, 70 rejected (5.6%)
[2026-02-24 10:30:46] [INFO] validator:   Error breakdown: {'invalid_price': 35, 'missing_identity_col': 20, 'invalid_seat_count': 15}
[2026-02-24 10:30:46] [INFO] db: Inserted 1180 valid flight offers
[2026-02-24 10:30:47] [INFO] run_all: Rejection details saved to output/validation/rejections_20260224_103046.json
```

---

## Next: After Phase 1 Passes

Once validation is solid, immediately move to **Step 2: Identity Tracking** (2a-2d), which takes ~1 hour and unlocks the ability to filter invalid records from analysis.
