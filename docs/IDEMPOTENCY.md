# Idempotency and Recovery Features

The Data Validator framework now includes comprehensive idempotency and recovery features that ensure operations can be safely repeated and pipelines can resume from any point of failure.

## Key Features

### 1. Rule-Level State Tracking

The framework now tracks completion state at the individual rule level, allowing for fine-grained resumption:

```python
from data_validator import DataValidator

# Validation will automatically resume from incomplete rules
validator = DataValidator("config.yaml")
summary = validator.validate_table(data, "my_table")

# Check if specific rules are completed
if validator._state.is_rule_completed("my_table", "completeness_check"):
    print("Rule already completed - using cached result")
```

### 2. Result Caching

Validation results are automatically cached to enable recovery from failures:

```python
# Results are cached after successful validation
summary1 = validator.validate_table(data, "my_table")

# Second call uses cached results (idempotent)
summary2 = validator.validate_table(data, "my_table")
assert summary2.total_rules == summary1.total_rules
```

### 3. Engine Resilience

Engines now include automatic retry logic for transient failures:

```yaml
engine:
  type: "duckdb"
  options:
    max_retries: 3      # Number of retry attempts
    retry_delay: 1.0    # Base delay between retries (exponential backoff)
```

### 4. Pipeline State Management

Enhanced state management with atomic operations:

```python
# Reset specific components
validator.reset_rule_state("table1", "rule1")  # Reset individual rule
validator.reset_table_state("table1")          # Reset entire table  
validator.reset_state()                        # Reset all state
```

### 5. Job Recovery

Databricks jobs now support automatic recovery and resumption:

```bash
# Run with automatic resumption (default)
data-validator-job --config config.yaml --sources sources.yaml --output report.json

# Disable resumption to force fresh execution
data-validator-job --config config.yaml --sources sources.yaml --output report.json --no-resume
```

## Configuration

### Pipeline State Configuration

Enable state persistence in your YAML configuration:

```yaml
version: "1.0"
pipeline:
  state_file: "pipeline_state.json"  # Path to persist state
engine:
  type: "duckdb"
  options:
    max_retries: 3        # Retry failed operations
    retry_delay: 1.0      # Base delay for exponential backoff
tables:
  - name: "customers"
    rules:
      - name: "id_check"
        rule_type: "completeness"
        column: "customer_id"
```

### State File Format

The state file stores detailed information about pipeline progress:

```json
{
  "customers": {
    "status": "completed",
    "completed_at": "2025-07-24T10:11:49.391678",
    "rules": {
      "id_check": {
        "status": "completed", 
        "completed_at": "2025-07-24T10:11:49.398754",
        "result": {
          "rule_name": "id_check",
          "passed": true,
          "success_rate": 1.0,
          "message": "All records valid"
        }
      }
    },
    "results": { /* cached validation summary */ }
  }
}
```

## Migration from Previous Versions

The framework automatically migrates old state file formats:

- Old format: `{"table1": "completed"}`
- New format: Nested structure with rule-level tracking

No manual migration is required - the system handles this transparently.

## Best Practices

### 1. Use State Files for Long-Running Pipelines

```python
# Enable state persistence for resumable pipelines
config = {
    "pipeline": {"state_file": "/path/to/state.json"},
    # ... rest of config
}
validator = DataValidator(config)
```

### 2. Configure Appropriate Retry Settings

```yaml
engine:
  options:
    max_retries: 3      # Conservative for production
    retry_delay: 2.0    # Allow time for transient issues to resolve
```

### 3. Handle Partial Failures

```python
try:
    results = validator.validate_all_tables(sources)
except Exception as e:
    # Completed tables are cached and won't be re-executed
    print(f"Pipeline failed: {e}")
    # Fix issues and re-run - only incomplete tables will be processed
```

### 4. Monitor State File Size

State files can grow large with many rules and tables. Consider:
- Regular cleanup of old state files
- Using relative paths for portability
- Backing up state files for disaster recovery

## Atomic Operations

All state updates are atomic to prevent corruption:

- State files are written to temporary location then moved
- Database operations use transactions where possible
- Failed operations don't leave partial state

## Error Handling

The framework distinguishes between:

- **Transient errors**: Automatically retried (connection issues, timeouts)
- **Permanent errors**: Logged and require manual intervention (invalid SQL, missing columns)

## Performance Considerations

Idempotency features add minimal overhead:

- State checks are fast (file-based lookup)
- Cached results avoid re-computation
- Network/database load is reduced for completed operations

## Troubleshooting

### State File Corruption

```python
# Reset corrupted state
validator.reset_state()

# Or manually delete the state file
import os
os.remove("pipeline_state.json")
```

### Rules Not Resuming

Check that:
1. State file path is configured correctly
2. Rule names are consistent between runs
3. State file has proper permissions

### Memory Usage

For large pipelines, consider:
- Periodic state cleanup
- Splitting large tables into smaller chunks
- Using external result storage for very large datasets