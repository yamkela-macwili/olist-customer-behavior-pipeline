"""
Data quality checks: referential integrity and null thresholds.

Implemented with plain SQLAlchemy — no Great Expectations dependency.
GX integration is planned as a future milestone (see docs/DECISIONS.md).

Design principle: every check raises ValueError on failure rather than
logging a warning and continuing. A DQ check that lets bad data through
is decorative, not functional.
"""

import logging

from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def _qualify(table: str, schema: str = None) -> str:
    return f'"{schema}"."{table}"' if schema else table


def check_referential_integrity(
    engine: Engine,
    child_table: str,
    child_fk_column: str,
    parent_table: str,
    parent_pk_column: str,
    child_schema: str = None,
    parent_schema: str = None,
) -> None:
    """Raise ValueError if any row in `child_table` has a FK value that does
    not exist in `parent_table`.

    Uses a LEFT JOIN anti-pattern (WHERE parent PK IS NULL) rather than a
    NOT IN subquery — NOT IN is O(n*m) and degrades badly at Olist's row
    counts; LEFT JOIN anti-join is O(n log n) with an index.
    """
    child = _qualify(child_table, child_schema)
    parent = _qualify(parent_table, parent_schema)

    query = f"""
        SELECT COUNT(*) FROM {child} c
        LEFT JOIN {parent} p ON c.{child_fk_column} = p.{parent_pk_column}
        WHERE p.{parent_pk_column} IS NULL
    """
    with engine.connect() as conn:
        orphaned = conn.exec_driver_sql(query).scalar()

    if orphaned > 0:
        raise ValueError(
            f"Referential integrity violation: {orphaned} row(s) in "
            f"{child_table}.{child_fk_column} have no matching "
            f"{parent_table}.{parent_pk_column}"
        )

    logger.info(
        "FK check passed: %s.%s → %s.%s",
        child_table, child_fk_column, parent_table, parent_pk_column,
    )


def check_null_thresholds(
    engine: Engine,
    table: str,
    column: str,
    max_null_fraction: float,
    schema: str = None,
) -> None:
    """Raise ValueError if the null fraction in `column` exceeds `max_null_fraction`.

    `max_null_fraction=0.0` means zero nulls tolerated. Values are expressed
    as fractions (0.05 = 5%), not percentages.
    """
    qualified = _qualify(table, schema)

    query = f"""
        SELECT
            COUNT(*) FILTER (WHERE "{column}" IS NULL)::float
            / NULLIF(COUNT(*), 0)
            AS null_fraction
        FROM {qualified}
    """
    with engine.connect() as conn:
        null_fraction = conn.exec_driver_sql(query).scalar() or 0.0

    if null_fraction > max_null_fraction:
        raise ValueError(
            f"Null threshold exceeded for {table}.{column}: "
            f"{null_fraction:.2%} nulls (max allowed: {max_null_fraction:.2%})"
        )

    logger.info(
        "Null check passed: %s.%s (%.2f%% nulls, threshold %.2f%%)",
        table, column, null_fraction * 100, max_null_fraction * 100,
    )
