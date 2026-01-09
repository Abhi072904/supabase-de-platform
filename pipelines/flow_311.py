from prefect import flow, task
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = REPO_ROOT / "dbt" / "de_platform"

@task(retries=2, retry_delay_seconds=30)
def run_ingest():
    subprocess.check_call([sys.executable, str(REPO_ROOT / "pipelines" / "ingest_311.py")])

@task
def run_dbt_run():
    subprocess.check_call(["dbt", "run"], cwd=str(DBT_DIR))

@task
def run_dbt_test():
    subprocess.check_call(["dbt", "test"], cwd=str(DBT_DIR))

@flow(name="nyc-311-end-to-end")
def pipeline():
    run_ingest()
    run_dbt_run()
    run_dbt_test()

if __name__ == "__main__":
    pipeline()
