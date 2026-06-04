import time
import subprocess
import os
from datetime import datetime

# Configuration
SPARK_MASTER = "spark://spark-master:7077"
PROCESSING_DIR = "/app/processing"
SERVING_DIR = "/app/serving"
POSTGRES_JAR = "/app/data/jars/postgresql.jar"

# Session-level statistics (accumulated across all iterations until process restart)
session_stats = {
    "total_runs": 0,
    "successful_runs": 0,
    "failed_runs": 0,
    "session_start": datetime.now(),
}


def run_spark_job_with_retry(job_path, max_retries=3, extra_jars=None):
    """Submit a Spark job with exponential backoff retry.

    Logs each attempt with attempt number and wait time.
    Returns True on success, False after all retries are exhausted.
    """
    job_name = os.path.basename(job_path)
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--name", f"ETL_{job_name}",
    ]
    if extra_jars:
        cmd += ["--jars", extra_jars]
    cmd.append(job_path)

    for attempt in range(1, max_retries + 1):
        print(f"  [Attempt {attempt}/{max_retries}] Submitting: {job_name}")
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(f"  [OK] {job_name} completed successfully on attempt {attempt}.")
            return True
        except subprocess.CalledProcessError as e:
            print(f"  [FAIL] {job_name} failed on attempt {attempt}.")
            if e.stderr:
                # Print last 20 lines of stderr to keep output manageable
                stderr_lines = e.stderr.strip().splitlines()
                tail = stderr_lines[-20:] if len(stderr_lines) > 20 else stderr_lines
                print("  --- stderr (tail) ---")
                for line in tail:
                    print(f"  {line}")
                print("  --- end stderr ---")

            if attempt < max_retries:
                wait_seconds = 10 * (2 ** (attempt - 1))  # 10s, 20s, 40s
                print(f"  Retrying in {wait_seconds}s...")
                time.sleep(wait_seconds)
            else:
                print(f"  [CRITICAL] {job_name} exhausted all {max_retries} retries.")

    return False


def main():
    print("=" * 60)
    print("Initializing Airflow-Lite Orchestrator")
    print(f"Session started at {session_stats['session_start'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Spark Master : {SPARK_MASTER}")
    print(f"Processing   : {PROCESSING_DIR}")
    print(f"Serving      : {SERVING_DIR}")
    print("=" * 60)

    # Pipeline stages in execution order.
    # Tuples of (full_job_path, is_critical).
    # A critical failure aborts the remainder of that iteration.
    processing_jobs = [
        (f"{PROCESSING_DIR}/job_bronze.py", True),
        (f"{PROCESSING_DIR}/job_silver.py", True),
        (f"{PROCESSING_DIR}/job_gold.py",   True),
    ]
    serving_jobs = [
        (f"{SERVING_DIR}/load_to_postgres.py", False, POSTGRES_JAR),
        (f"{SERVING_DIR}/export_for_bi.py",    False, None),
    ]

    while True:
        session_stats["total_runs"] += 1
        run_number = session_stats["total_runs"]
        run_start = datetime.now()
        run_timestamp = run_start.strftime("%Y-%m-%d %H:%M:%S")

        print()
        print("=" * 60)
        print(f"[RUN #{run_number} | {run_timestamp}]")
        print("=" * 60)

        run_succeeded = True
        failed_job = None

        # --- Processing jobs (Bronze → Silver → Gold) ---
        for job_path, is_critical in processing_jobs:
            job_name = os.path.basename(job_path)
            success = run_spark_job_with_retry(job_path)
            if not success:
                run_succeeded = False
                failed_job = job_name
                if is_critical:
                    print(f"  [ABORT] Critical job {job_name} failed. "
                          "Skipping remaining jobs for this run.")
                    break

        # --- Serving jobs (only if all processing jobs succeeded) ---
        if run_succeeded:
            for job_path, is_critical, extra_jars in serving_jobs:
                job_name = os.path.basename(job_path)
                success = run_spark_job_with_retry(job_path, extra_jars=extra_jars)
                if not success:
                    # Serving failures are non-critical; record but continue
                    run_succeeded = False
                    failed_job = job_name
                    print(f"  [WARN] Serving job {job_name} failed. "
                          "Data may not be visible in Grafana this cycle.")
        else:
            print("  [SKIP] Serving jobs skipped due to upstream processing failure.")

        # --- Run summary ---
        run_end = datetime.now()
        duration_seconds = (run_end - run_start).total_seconds()

        if run_succeeded:
            session_stats["successful_runs"] += 1
            status_label = "SUCCESS"
        else:
            session_stats["failed_runs"] += 1
            status_label = f"FAILED (at {failed_job})"

        print()
        print(f"[RUN #{run_number} SUMMARY]")
        print(f"  Status   : {status_label}")
        print(f"  Duration : {duration_seconds:.1f}s")
        print(f"  Session  : {session_stats['total_runs']} runs total | "
              f"{session_stats['successful_runs']} succeeded | "
              f"{session_stats['failed_runs']} failed")

        print(f"\nSleeping 60s until next run...")
        time.sleep(60)


if __name__ == "__main__":
    main()
