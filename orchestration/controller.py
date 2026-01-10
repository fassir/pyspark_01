import time
import subprocess
import os

# Configuration
SPARK_MASTER = "spark://spark-master:7077"
PROCESSING_DIR = "/app/processing"

# We assume this script runs in a container that has 'spark-submit' on PATH
# and shares the 'processing' volume.

def run_spark_job(job_file):
    print(f"--- Submitting Job: {job_file} ---")
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--name", f"ETL_{job_file}",

        f"{PROCESSING_DIR}/{job_file}"
    ]
    
    try:
        # Capture output to see Spark logs if needed
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"Job {job_file} Success.")
        # print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Job {job_file} Failed!")
        print(e.stderr)

def main():
    print("Initializing Airflow-Lite Orchestrator...")
    # Simple loop simulation
    while True:
        print("\nStarting ETL Pipeline Run...")
        
        # 1. Bronze
        run_spark_job("job_bronze.py")
        
        # 2. Silver
        run_spark_job("job_silver.py")
        
        # 3. Gold
        run_spark_job("job_gold.py")
        
        # 4. Serving
        # The loading script uses Spark context for reading Parquet, so we submit it too.
        # But wait, the serving/load_to_postgres.py needs psycopg2 which might not be in standard spark image.
        # Ideally we build a custom image or install it. 
        # For this demo, let's assume we install it in the Dockerfile.
        run_spark_job("../serving/load_to_postgres.py")
        
        # 5. Export for PowerBI/Tableau (Manual file based)
        run_spark_job("../serving/export_for_bi.py")


        print("Pipeline Run Complete. Sleeping for 60s...")
        time.sleep(60)

if __name__ == "__main__":
    main()
