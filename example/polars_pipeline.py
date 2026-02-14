"""Using Result with Polars DataFrames for safe data pipelines."""

import polars as pl

from polars_result import PolarsResultError, Result, ValidationError

# ── Simulate raw data (e.g. from Google Sheets) ─────────────────────────────

raw_data = {
    "vessel": ["MV Pacific Star", "FV Albatross", "MV Pacific Star", "FV Coral"],
    "container": ["MSCU1234567", "TCLU9876543", "BAD-ID", "MSCU1112233"],
    "tonnes": [12.5, None, 8.3, 15.0],
    "service": ["stevedoring", "cold_storage", "stevedoring", "unknown"],
}


# ── Pipeline steps as Result-returning functions ─────────────────────────────

def load_data(data: dict) -> Result[pl.DataFrame, Exception]:
    """Wrap raw data into a DataFrame."""
    try:
        df = pl.DataFrame(data)
        return Result.ok(df)
    except Exception as e:
        return Result.err(PolarsResultError(f"Failed to load data: {e}"))


def validate_containers(df: pl.DataFrame) -> Result[pl.DataFrame, Exception]:
    """Flag rows with invalid container numbers (must be 11 alphanumeric chars)."""
    validated = df.with_columns(
        pl.col("container")
        .str.contains(r"^[A-Z]{4}\d{7}$")
        .alias("valid_container")
    )
    invalid_count = validated.filter(~pl.col("valid_container")).height
    if invalid_count > 0:
        print(f"  ⚠ {invalid_count} invalid container(s) found — flagged, not dropped")
    return Result.ok(validated)


def fill_missing_tonnes(df: pl.DataFrame) -> pl.DataFrame:
    """Fill null tonnes with 0 (pure transform, used with .map)."""
    return df.with_columns(pl.col("tonnes").fill_null(0.0))


def validate_services(df: pl.DataFrame) -> Result[pl.DataFrame, Exception]:
    """Ensure all services are known."""
    known = {"stevedoring", "cold_storage", "container_handling"}
    actual = set(df["service"].unique().to_list())
    unknown = actual - known
    if unknown:
        return Result.err(
            ValidationError(f"Unknown service(s): {unknown}")
        )
    return Result.ok(df)


# ── Run the pipeline ─────────────────────────────────────────────────────────

print("── Running data pipeline ──\n")

result = (
    load_data(raw_data)
    .bind(validate_containers)
    .map(fill_missing_tonnes)
    .bind(validate_services)
)

if result.is_ok():
    print("\n✓ Pipeline succeeded:")
    print(result.unwrap())
else:
    print(f"\n✗ Pipeline failed: {result.unwrap_err()}")

# ── Recover and continue ────────────────────────────────────────────────────

print("\n── With error recovery ──\n")

KNOWN_SERVICES = {"stevedoring", "cold_storage", "container_handling"}

result_recovered = (
    load_data(raw_data)
    .bind(validate_containers)
    .map(fill_missing_tonnes)
    .bind(validate_services)
    .or_else(lambda _e: (
        load_data(raw_data)
        .bind(validate_containers)
        .map(fill_missing_tonnes)
        .map(lambda df: df.filter(pl.col("service").is_in(KNOWN_SERVICES)))
    ))
)

if result_recovered.is_ok():
    df = result_recovered.unwrap()
    print(f"✓ Recovered — {df.height} valid rows:")
    print(df)
