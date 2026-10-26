"""Contract validation and typing of CDC rows, expressed as Spark column expressions.

Input: a DataFrame with `row` as map<string,string> (Debezium after/before image as text).
Output columns: `_reasons` (array<string>, empty when valid) and typed contract columns.
Invalid values are never coerced: a row with any reason is quarantined as a whole.
"""

from __future__ import annotations

from typing import Any

from dataops.contracts import TableContract

INT_RE = r"^-?[0-9]{1,18}$"
TS_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+(Z|[+-][0-9]{2}:?[0-9]{2})$"

SPARK_TYPES = {
    "int": "bigint",
    "text": "string",
    "char2": "string",
    "char3": "string",
    "bool": "boolean",
    "timestamp": "timestamp",
}


def reason_exprs(contract: TableContract, op_col: str = "op") -> list[Any]:
    """One `when(...)` per possible violation; nulls are filtered out afterwards."""
    from pyspark.sql import functions as F

    is_upsert = F.col(op_col).isin("c", "u", "r")
    out = [
        F.when(F.col("row").isNull(), F.lit("missing row image")),
        F.when(F.col("row")[contract.key].isNull(), F.lit(f"missing key {contract.key}")),
    ]
    for c in contract.columns:
        v = F.col("row")[c.name]
        # Delete events carry only the primary key under REPLICA IDENTITY DEFAULT; Debezium
        # fills the other columns with placeholders ("" / epoch), so they are not validated.
        present = v.isNotNull() & (is_upsert | F.lit(c.name == contract.key))
        if c.required:
            out.append(F.when(is_upsert & ~present, F.lit(f"{c.name}: required value missing")))
        if c.type == "int":
            out.append(
                F.when(
                    present & ~v.rlike(INT_RE),
                    F.concat(F.lit(f"{c.name}: expected integer, got "), F.substring(v, 1, 40)),
                )
            )
            if c.min is not None:
                out.append(
                    F.when(
                        present & v.rlike(INT_RE) & (v.cast("bigint") < c.min),
                        F.concat(F.lit(f"{c.name}: below minimum {c.min}: "), v),
                    )
                )
        elif c.type == "timestamp":
            out.append(
                F.when(
                    present & ~v.rlike(TS_RE),
                    F.concat(
                        F.lit(f"{c.name}: expected ISO-8601 timestamp, got "), F.substring(v, 1, 40)
                    ),
                )
            )
        elif c.type == "bool":
            out.append(
                F.when(
                    present & ~v.isin("true", "false"),
                    F.concat(F.lit(f"{c.name}: expected boolean, got "), v),
                )
            )
        elif c.type in ("char2", "char3"):
            n = 2 if c.type == "char2" else 3
            out.append(
                F.when(
                    present & (F.length(v) != n),
                    F.concat(F.lit(f"{c.name}: expected {n} characters, got "), v),
                )
            )
        if c.allowed:
            out.append(
                F.when(
                    present & ~v.isin(*c.allowed),
                    F.concat(F.lit(f"{c.name}: value not allowed: "), v),
                )
            )
    return out


def with_validation(df: Any, contract: TableContract, op_col: str = "op") -> Any:
    from pyspark.sql import functions as F

    reasons = F.filter(F.array(*reason_exprs(contract, op_col)), lambda x: x.isNotNull())
    return df.withColumn("_reasons", reasons)


def typed_columns(contract: TableContract, op_col: str = "op") -> list[Any]:
    """Typed contract columns. For deletes only the key is kept; the other values are
    Debezium placeholders and are nulled rather than stored."""
    from pyspark.sql import functions as F

    cols = []
    for c in contract.columns:
        v = F.col("row")[c.name]
        typed = F.to_timestamp(v) if c.type == "timestamp" else v.cast(SPARK_TYPES[c.type])
        if c.name != contract.key:
            typed = F.when(F.col(op_col) != "d", typed)
        cols.append(typed.alias(c.name))
    return cols


def silver_schema_ddl(contract: TableContract) -> str:
    cols = [f"`{c.name}` {SPARK_TYPES[c.type]}" for c in contract.columns]
    meta = [
        "_lsn bigint",
        "_op string",
        "_is_deleted boolean",
        "_source_ts timestamp",
        "_bronze_topic string",
        "_bronze_offset bigint",
        "_silver_batch_id bigint",
        "_updated_at timestamp",
    ]
    return ", ".join(cols + meta)
