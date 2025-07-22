"""Demo for running data validation as a Databricks job."""

from pathlib import Path
import json
import tempfile
from data_validator.databricks_job import run_job


if __name__ == "__main__":
    # Paths
    config_path = Path(__file__).parent / "duckdb_config.yaml"

    # Create simple data source mapping
    data_path = Path(tempfile.gettempdir()) / "customers.csv"
    data_path.write_text("id,name\n1,Alice\n2,Bob\n")

    sources_yaml = Path(tempfile.gettempdir()) / "sources.yaml"
    sources_yaml.write_text(f"customers: {data_path}\n")

    output_json = Path(tempfile.gettempdir()) / "validation_report.json"

    run_job(str(config_path), str(sources_yaml), str(output_json))

    print(output_json.read_text())
