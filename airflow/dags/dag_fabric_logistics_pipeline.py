from airflow import DAG
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Thay thế bằng ID thực tế bạn lấy từ URL trong Fabric
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID", "your_workspace_id")
BRONZE_SPARK_JOB_ID = os.getenv("FABRIC_BRONZE_SPARK_JOB_ID", "your_bronze_job_id")
SILVER_SPARK_JOB_ID = os.getenv("FABRIC_SILVER_SPARK_JOB_ID", "your_silver_job_id")

with DAG(
    dag_id="fabric_logistics_medallion_pipeline",
    start_date=datetime(2026, 9, 21),
    schedule_interval="@daily",
    catchup=False,
    tags=["fabric", "spark", "dbt", "etl"]
) as dag:

    # 1. Chạy Spark Job sinh dữ liệu thô (Bronze)
    run_bronze_job = MSFabricRunJobOperator(
        task_id="trigger_raw_to_bronze_sjd",
        workspace_id=WORKSPACE_ID,
        item_id=BRONZE_SPARK_JOB_ID,
        job_type="SparkJob",
        fabric_conn_id="fabric_default",
        wait_for_termination=True,
        deferrable=False
    )

    # 2. Chạy Spark Job làm sạch dữ liệu (Silver)
    run_silver_job = MSFabricRunJobOperator(
        task_id="trigger_bronze_to_silver_sjd",
        workspace_id=WORKSPACE_ID,
        item_id=SILVER_SPARK_JOB_ID,
        job_type="SparkJob",
        fabric_conn_id="fabric_default",
        wait_for_termination=True,
        deferrable=False
    )

    # 3. Chạy dbt-fabric để transform dữ liệu (Gold)
    # Giả định dbt project đã được cấu hình trong image Airflow hoặc mount volume
    dbt_project_dir = "/opt/airflow/dags/repo/dbt_project" # Đường dẫn thực tế tùy môi trường
    run_dbt_gold = BashOperator(
        task_id="run_dbt_silver_to_gold",
        bash_command=f"cd {dbt_project_dir} && dbt build --target prod --profiles-dir .",
    )

    run_bronze_job >> run_silver_job >> run_dbt_gold
