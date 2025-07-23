# In-Memory DuckDB Validation

This guide demonstrates how to run validations using an in-memory DuckDB instance where all validation rules are defined as SQL snippets. This approach is fully portable as only SQL is required.

## Running the demo

```bash
python examples/duckdb_sql_demo.py
```

The demo loads `examples/customers.csv` and validates it using the configuration in `examples/sql_rules_config.yaml`.

`require_sql_rules: true` in the configuration ensures every rule supplies a SQL `expression`.
