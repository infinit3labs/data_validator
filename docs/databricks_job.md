# Running Data Validation as a Databricks Job

The `data_validator.databricks_job` module provides a simple entry point for executing
validation rules inside a Databricks job. It expects a YAML configuration file and a
YAML mapping of table names to data sources. The resulting validation report is
written to a JSON file which can be stored on DBFS or any mounted location.

## Usage

```bash
python -m data_validator.databricks_job \
  --config /dbfs/path/to/config.yaml \
  --sources /dbfs/path/to/sources.yaml \
  --output /dbfs/path/to/report.json
```

- `--config` – path to the validation configuration YAML file.
- `--sources` – YAML file mapping table names to paths or tables.
- `--output` – destination path for the JSON validation report.

This command can be used as the main task in a Databricks job.

## Example Sources File

```yaml
customers: /dbfs/data/customers.csv
orders: /dbfs/data/orders.parquet
```

## Output

The command produces a JSON report summarising validation results for each table.
This report can be further processed or stored for audit purposes.
