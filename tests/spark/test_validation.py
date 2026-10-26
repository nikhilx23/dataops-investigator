"""Silver contract validation on a local Spark session (no Kafka or Delta tables needed).

Marked `spark`: needs Java and the PySpark/Delta packages (jars resolved from Maven once).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from dataops.contracts import load  # noqa: E402
from dataops.streaming.validation import typed_columns, with_validation  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from dataops.streaming.session import get_spark

    return get_spark("dataops-tests")


def _df(spark, rows):
    return spark.createDataFrame(rows, "op string, row map<string,string>")


GOOD = {
    "payment_id": "1",
    "order_id": "10",
    "attempt_number": "1",
    "status": "succeeded",
    "method": "card",
    "amount_cents": "1299",
    "created_at": "2026-03-02T10:00:00Z",
    "captured_at": "2026-03-02T10:01:00Z",
    "failure_reason": None,
    "updated_at": "2026-03-02T10:01:00Z",
}


def reasons(spark, op, row, table="payments"):
    out = with_validation(_df(spark, [(op, row)]), load()[table]).collect()[0]["_reasons"]
    return list(out)


def test_valid_row_has_no_reasons(spark):
    assert reasons(spark, "c", GOOD) == []


def test_breaking_type_change_is_rejected_not_coerced(spark):
    # The source starts sending dollars as a decimal string instead of integer cents.
    bad = dict(GOOD, amount_cents="12.99")
    r = reasons(spark, "u", bad)
    assert r == ["amount_cents: expected integer, got 12.99"]


def test_required_missing_allowed_values_and_minimum(spark):
    bad = dict(GOOD, method=None, status="settled", amount_cents="0")
    r = reasons(spark, "c", bad)
    assert "method: required value missing" in r
    assert "status: value not allowed: settled" in r
    assert "amount_cents: below minimum 1: 0" in r


def test_bad_timestamp_and_boolean(spark):
    r = reasons(spark, "c", dict(GOOD, created_at="03/02/2026"))
    assert r == ["created_at: expected ISO-8601 timestamp, got 03/02/2026"]
    product = {
        "product_id": "1",
        "sku": "S",
        "name": "N",
        "category": "c",
        "list_price_cents": "100",
        "is_active": "yes",
        "created_at": "2026-03-02T10:00:00Z",
        "updated_at": "2026-03-02T10:00:00Z",
    }
    assert reasons(spark, "c", product, "products") == ["is_active: expected boolean, got yes"]


def test_delete_validates_key_only(spark):
    # Debezium delete images carry placeholders for non-key columns (REPLICA IDENTITY DEFAULT).
    placeholder = {k: "" for k in GOOD} | {"payment_id": "1", "created_at": "1970-01-01T00:00:00Z"}
    assert reasons(spark, "d", placeholder) == []
    assert reasons(spark, "d", {"order_id": "10"}) == ["missing key payment_id"]


def test_typed_columns_null_non_key_values_for_deletes(spark):
    c = load()["payments"]
    row = (
        _df(spark, [("d", {"payment_id": "7", "amount_cents": ""})])
        .select(*typed_columns(c))
        .collect()[0]
    )
    assert row["payment_id"] == 7 and row["amount_cents"] is None
