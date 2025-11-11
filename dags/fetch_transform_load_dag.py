from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import sys
import os
import datetime as dt

# 🧩 Allow imports from scripts folder
sys.path.append("/opt/airflow/dags/scripts")

# ✅ Import ETL scripts
from fetch import fetch_currency_data
from transform import transform_from_gcs_to_df, upload_transformed_to_gcs
from load_to_bigquery import load_from_gcs_to_bq

# ==============================
# Default Args
# ==============================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    # <-- add both emails here for failure alerts
    'email': ['abdulllah02003@gmail.com', 'ahmed.azab201829@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ==============================
# DAG Definition
# ==============================
with DAG(
    dag_id='fetch_transform_load_dag',
    description='💹 Automated Currency ETL: Fetch → Transform → Load → Notify',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['ETL', 'currency', 'BigQuery', 'email']
) as dag:

    # 1️⃣ Fetch Stage
    def fetch_wrapper(**context):
        conf = context.get("dag_run").conf or {}
        base_currency = conf.get("base_currency", "USD")
        target_currency = conf.get("target_currency", None)

        print(f"🌍 Fetching data for: {base_currency} → {target_currency or 'ALL'}")
        gcs_file = fetch_currency_data(base_currency, target_currency)
        print(f"✅ GCS file uploaded: {gcs_file}")

        return {"gcs_file": gcs_file, "base_currency": base_currency, "target_currency": target_currency}

    fetch_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_wrapper,
        provide_context=True,
    )

    # 2️⃣ Transform Stage
    def transform_wrapper(**context):
        ti = context['ti']
        xcom_data = ti.xcom_pull(task_ids='fetch_data')
        gcs_file = xcom_data.get("gcs_file")

        print(f"🔄 Starting transformation for file: {gcs_file}")
        df = transform_from_gcs_to_df(gcs_file)

        cairo_tz = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"clean1/exchangerate/live/transformed_{cairo_tz}.csv"
        upload_transformed_to_gcs(df, output_file)
        print(f"✅ Transformed file uploaded: {output_file}")

        return output_file

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_wrapper,
        provide_context=True,
    )

    # 3️⃣ Load Stage
    def load_wrapper(**context):
        transformed_file = context['ti'].xcom_pull(task_ids='transform_data')
        print(f"🚀 Loading transformed data into BigQuery: {transformed_file}")
        load_from_gcs_to_bq(transformed_file)
        print("✅ BigQuery Load Complete!")

    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_wrapper,
        provide_context=True,
    )

    # 4️⃣ Email Notification Stage
    def build_email_content(**context):
        xcom_data = context['ti'].xcom_pull(task_ids='fetch_data')
        base_currency = xcom_data.get("base_currency")
        target_currency = xcom_data.get("target_currency", "ALL")

        # 🧠 Connect to BigQuery
        client = bigquery.Client()
        dataset = "currency"
        current_table = f"{client.project}.{dataset}.current_rates"
        historical_table = f"{client.project}.{dataset}.historical_rates"

        # ✅ Get latest rate
        latest_rate = None
        try:
            latest_query = f"""
                SELECT rate, timestamp
                FROM `{current_table}`
                WHERE base_currency = '{base_currency}' AND target_currency = '{target_currency}'
                ORDER BY timestamp DESC
                LIMIT 1
            """
            latest_df = client.query(latest_query).to_dataframe()
            latest_rate = latest_df["rate"].iloc[0] if not latest_df.empty else None
        except Exception:
            latest_rate = None

        # ✅ Compare with rate from 24 hours ago
        prev_rate = None
        try:
            compare_query = f"""
                SELECT rate, timestamp
                FROM `{historical_table}`
                WHERE base_currency = '{base_currency}' AND target_currency = '{target_currency}'
                  AND timestamp <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 0 SECOND)
                  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
                ORDER BY timestamp ASC
                LIMIT 1
            """
            compare_df = client.query(compare_query).to_dataframe()
            prev_rate = compare_df["rate"].iloc[0] if not compare_df.empty else None
        except Exception:
            prev_rate = None

        # ✅ Compute change
        change_html = ""
        if latest_rate is not None and prev_rate is not None:
            diff = latest_rate - prev_rate
            percent = (diff / prev_rate) * 100 if prev_rate != 0 else 0
            direction = "🔺 Increased" if diff > 0 else "🔻 Decreased" if diff < 0 else "➖ No change"
            change_html = f"<p><b>📊 Last 24h:</b> {direction} by {abs(percent):.2f}% (from {prev_rate:.4f} → {latest_rate:.4f})</p>"
        elif latest_rate is not None:
            change_html = f"<p>💱 Current rate: 1 {base_currency} = {latest_rate:.4f} {target_currency}</p>"
        else:
            change_html = "<p>⚠️ No recent rate data available.</p>"

        return f"""
        <html>
            <body style="font-family:Arial,sans-serif; color:#333;">
                <h2>🎉 Exchange Rate ETL Completed Successfully!</h2>
                <p>The automated ETL pipeline finished successfully for:</p>
                <h3>💱 {base_currency} → {target_currency}</h3>
                {change_html}
                <h4>✅ Pipeline Summary:</h4>
                <ul>
                    <li><b>Fetch:</b> Data retrieved from exchangerate.host API</li>
                    <li><b>Transform:</b> Cleaned and formatted in GCS</li>
                    <li><b>Load:</b> Loaded into BigQuery (historical + current tables)</li>
                </ul>
                <p><b>Execution Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><b>Status:</b> SUCCESS ✅</p>
                <hr>
                <p>📨 Sent automatically by <b>Airflow Currency ETL</b></p>
            </body>
        </html>
        """

    build_email_task = PythonOperator(
        task_id="build_email",
        python_callable=build_email_content,
        provide_context=True
    )

    # <-- add both emails here as recipients for the summary email
    send_email_task = EmailOperator(
        task_id='send_summary_email',
        to=['abdulllah02003@gmail.com', 'ahmed.azab201829@gmail.com'],
        subject='✅ Exchange Rate ETL Completed Successfully',
        html_content="{{ task_instance.xcom_pull(task_ids='build_email') }}"
    )

    # 5️⃣ Dependencies
    fetch_task >> transform_task >> load_task >> build_email_task >> send_email_task
