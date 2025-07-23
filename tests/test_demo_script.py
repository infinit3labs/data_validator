from examples.demo_validation import run_demo


def test_run_demo():
    summary = run_demo()
    assert summary.table_name == "users"
    assert summary.total_rules == 3
