from dbt.cli.main import dbtRunner, dbtRunnerResult

PROJECT_DIR = "dbt"
PROFILE_DIR = "dbt"

METRIC__MODELS = {
    "customer_segments": "metric__customer_segments.sql",
}


def build_models():
    """dbt run to build models."""
    dbt = dbtRunner()

    result: dbtRunnerResult = dbt.invoke(
        [
            "run",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ]
    )

    # Check the results
    if not result.success:
        raise result.exception


def analyze(model: str):
    """Show models analysis

    Args:
        model (str): either `commits` or `issues`

    Raises:
        result.exception: error when run dbt show --select <model>
    """
    dbt = dbtRunner()

    result: dbtRunnerResult = dbt.invoke(
        [
            "show",
            "--select",
            METRIC__MODELS[model],
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ]
    )

    print (f"Model is short demonstrated from table: {METRIC__MODELS[model]}")

    # Check the results
    if not result.success:
        raise result.exception
