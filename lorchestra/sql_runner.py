"""SQL runner for lorchestra CLI.

Provides read-only SQL execution with safety gates and placeholder substitution.

All queries must pass through the read-only validator which:
1. Strips comments
2. Requires query to start with SELECT or WITH
3. Rejects mutating keywords (INSERT, UPDATE, DELETE, etc.)
4. Rejects multi-statement SQL (only trailing ; allowed)
"""

import re
import click
from google.cloud import bigquery
from lorchestra.config import LorchestraConfig


# Mutating SQL keywords - these are NOT allowed
MUTATING_KEYWORDS = [
    'insert',
    'update',
    'delete',
    'merge',
    'truncate',
    'create',
    'drop',
    'alter',
    'grant',
    'revoke',
    'call',
    'execute',
]


def validate_readonly_sql(sql: str) -> None:
    """Validate that SQL is read-only.

    Args:
        sql: SQL query string

    Raises:
        click.UsageError: If query is not read-only
    """
    # 1. Strip comments
    cleaned = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)

    # 2. Normalize: lowercase, collapse whitespace
    normalized = ' '.join(cleaned.lower().split())

    if not normalized:
        raise click.UsageError("Query is empty after stripping comments")

    # 3. Check starts with SELECT or WITH
    if not (normalized.startswith('select') or normalized.startswith('with')):
        raise click.UsageError(
            "Query must start with SELECT or WITH. "
            f"Found: {normalized[:50]}..."
        )

    # 4. Check for mutating keywords (whole words)
    for keyword in MUTATING_KEYWORDS:
        if re.search(rf'\b{keyword}\b', normalized):
            raise click.UsageError(
                f"Refusing to run non-read-only SQL. "
                f"Detected mutating keyword: {keyword}"
            )

    # 5. Check for multi-statement (semicolon not at end)
    stripped = sql.strip()
    if stripped and ';' in stripped[:-1]:
        raise click.UsageError(
            "Multi-statement SQL not allowed. "
            "Only a single trailing semicolon is permitted."
        )


def substitute_placeholders(
    sql: str,
    config: LorchestraConfig,
    extra: dict[str, str] | None = None,
) -> str:
    """Substitute ${PLACEHOLDER} values in SQL.

    Supports:
    - ${PROJECT}
    - ${DATASET_RAW}
    - ${DATASET_CANONICAL}
    - ${DATASET_DERIVED}
    - ${DATASET} (Legacy/Generic - maps to dataset_raw)

    Args:
        sql: SQL with placeholders
        config: Configuration object
        extra: Additional placeholders (e.g., {"DAYS": "7"})

    Returns:
        SQL with placeholders replaced
    """
    sql = sql.replace('${PROJECT}', config.project)
    sql = sql.replace('${DATASET_RAW}', config.dataset_raw)
    sql = sql.replace('${DATASET_CANONICAL}', config.dataset_canonical)
    sql = sql.replace('${DATASET_DERIVED}', config.dataset_derived)
    
    # Fallback/Legacy
    sql = sql.replace('${DATASET}', config.dataset_raw)

    if extra:
        for key, value in extra.items():
            sql = sql.replace(f'${{{key}}}', str(value))

    return sql


# Known tables that can be auto-qualified using specific datasets
TABLE_MAPPING = {
    'event_log': 'dataset_raw',
    'raw_objects': 'dataset_raw',
    'canonical_objects': 'dataset_canonical',
    'measurement_events': 'dataset_derived',
    'observations': 'dataset_derived',
}


def auto_qualify_tables(sql: str, config: LorchestraConfig) -> str:
    """Auto-qualify bare table names with project.dataset prefix.

    Replaces bare references like ``FROM event_log`` with
    ``FROM `project.dataset.event_log```, using the correct dataset
    for the table type.

    Only qualifies known tables to avoid false positives.
    """
    for table, ds_field in TABLE_MAPPING.items():
        dataset = getattr(config, ds_field)
        
        # Match: FROM/JOIN table_name (not already qualified)
        # Negative lookbehind for `.` or backtick to avoid re-qualifying
        pattern = rf'(?<![`.])(\\bFROM\\s+)({table})(\\s|$|,)'
        replacement = rf'\\1`{config.project}.{dataset}.{table}`\\3'
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

        pattern = rf'(?<![`.])(\\bJOIN\\s+)({table})(\\s|$)'
        replacement = rf'\\1`{config.project}.{dataset}.{table}`\\3'
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    return sql


def run_sql_query(
    sql: str,
    config: LorchestraConfig,
    extra_placeholders: dict[str, str] | None = None,
) -> None:
    """Execute read-only SQL and print results as aligned table.

    Args:
        sql: SQL query (may contain ${PROJECT}, ${DATASET} placeholders)
        config: Configuration object
        extra_placeholders: Additional placeholders to substitute

    Raises:
        click.UsageError: If query fails validation
    """
    # Substitute placeholders
    sql = substitute_placeholders(sql, config, extra_placeholders)

    # Auto-qualify known table names
    sql = auto_qualify_tables(sql, config)

    # Validate read-only
    validate_readonly_sql(sql)

    # Execute
    client = bigquery.Client(project=config.project)

    try:
        result = client.query(sql).result()
    except Exception as e:
        raise click.ClickException(f"Query failed: {e}")

    # Get rows and schema
    rows = list(result)
    if not rows:
        click.echo("No results.")
        return

    schema = result.schema
    col_names = [field.name for field in schema]

    # Calculate column widths
    widths = {col: len(col) for col in col_names}
    for row in rows:
        for col in col_names:
            val = row[col]
            # Handle None and format timestamps
            if val is None:
                val_str = "NULL"
            elif hasattr(val, 'isoformat'):
                val_str = val.isoformat()
            else:
                val_str = str(val)
            widths[col] = max(widths[col], len(val_str))

    # Print header
    header = '  '.join(col.ljust(widths[col]) for col in col_names)
    click.echo(header)
    click.echo('-' * len(header))

    # Print rows
    for row in rows:
        cells = []
        for col in col_names:
            val = row[col]
            if val is None:
                val_str = "NULL"
            elif hasattr(val, 'isoformat'):
                val_str = val.isoformat()
            else:
                val_str = str(val)
            cells.append(val_str.ljust(widths[col]))
        click.echo('  '.join(cells))

    # Print row count
    click.echo(f"\n({len(rows)} rows)")

