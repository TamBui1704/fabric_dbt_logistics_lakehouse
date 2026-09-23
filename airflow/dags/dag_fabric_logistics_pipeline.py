from airflow import DAG
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator
from airflow.operators.bash import BashOperator
import pendulum
import os

# Cấu hình múi giờ Việt Nam (UTC+7)
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Lấy ID trực tiếp từ biến môi trường (Environment Variables)
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID", "your_workspace_id")
BRONZE_SPARK_JOB_ID = os.getenv("FABRIC_BRONZE_SPARK_JOB_ID", "your_bronze_job_id")
SILVER_SPARK_JOB_ID = os.getenv("FABRIC_SILVER_SPARK_JOB_ID", "your_silver_job_id")

with DAG(
    dag_id="fabric_logistics_medallion_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    schedule_interval="0 5 * * *",  # Tự động chạy đúng 5:00 AM hàng ngày (giờ VN)
    catchup=False,                   # KHÔNG chạy bù các ngày trong quá khứ
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
    # (Tạm comment, khi nào bật Fabric API thì bỏ comment dấu # để dùng)
    # dbt_project_dir = "/opt/airflow/dags/repo/dbt_project"
    # run_dbt_gold = BashOperator(
    #     task_id="run_dbt_silver_to_gold",
    #     bash_command=f"cd {dbt_project_dir} && dbt build --target prod --profiles-dir .",
    # )

    # Điều phối thứ tự:
    # Nếu chạy cả dbt: run_bronze_job >> run_silver_job >> run_dbt_gold
    run_bronze_job >> run_silver_job
