import pytest

from dataops.contracts import _parse, load
from dataops.generator.generate import PRIMARY_KEYS, generate


def test_contract_covers_every_source_table_and_column():
    contracts = load()
    ds = generate(1, "tiny")
    assert set(contracts) == set(ds.tables)
    for table, rows in ds.tables.items():
        assert contracts[table].key == PRIMARY_KEYS[table]
        assert set(rows[0]) == set(contracts[table].column_names), table


def test_generated_data_satisfies_allowed_values_and_minimums():
    contracts = load()
    ds = generate(2, "small")
    for table, rows in ds.tables.items():
        for col in contracts[table].columns:
            for r in rows:
                v = r[col.name]
                if col.required:
                    assert v is not None, (table, col.name)
                if v is not None and col.allowed:
                    assert v in col.allowed, (table, col.name, v)
                if v is not None and col.min is not None:
                    assert v >= col.min, (table, col.name, v)


def test_parser_rejects_unknown_type_and_bad_key():
    doc = {
        "contract": "x",
        "version": 1,
        "tables": {"t": {"key": "id", "freshness_hours": 1, "columns": {"id": {"type": "uuid"}}}},
    }
    with pytest.raises(ValueError, match="unknown type"):
        _parse(doc)
    doc["tables"]["t"]["columns"] = {"other": {"type": "int"}}
    with pytest.raises(ValueError, match="key"):
        _parse(doc)
