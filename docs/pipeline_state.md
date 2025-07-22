# Pipeline State Management

The validator can persist progress so that pipelines can resume without repeating
work. Enable this feature by specifying a `state_file` under the `pipeline`
section of your YAML configuration:

```yaml
pipeline:
  state_file: "pipeline_state.json"
```

During `validate_all_tables` each table is marked as completed after successful
validation. On subsequent runs any tables already marked completed are skipped,
ensuring idempotent execution. Use `DataValidator.reset_state()` to clear the
state and reprocess all tables.

Run the demonstration script:

```bash
python examples/pipeline_demo.py
```
