from google.cloud import bigquery, storage
import pandas as pd
import io
import os
import tempfile
from datetime import datetime
import pytz

# إعداد بيانات Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/keys/gcp.json"

# ============================================================
# 🔹 التأكد من وجود الجدول في BigQuery
# ============================================================
def ensure_table_exists(client, table_id, schema):
    """يتأكد من وجود الجدول أو ينشئه في حالة عدم وجوده."""
    try:
        client.get_table(table_id)
        print(f"✅ Table {table_id} already exists.")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"🆕 Created new table: {table_id}")

# ============================================================
# 🔹 تحميل البيانات إلى BigQuery
# ============================================================
def load_to_bq(df):
    """Loads currency data into BigQuery (append + merge)."""
    client = bigquery.Client()
    project = client.project
    dataset = "currency"

    historical_table = f"{project}.{dataset}.historical_rates"
    current_table = f"{project}.{dataset}.current_rates"
    tmp_table = f"{project}.{dataset}.tmp_rates"

    print("🚀 Starting BigQuery load process...")

    # 🧹 تنظيف وتحضير البيانات
    if "target_currency" not in df.columns and "pair" in df.columns:
        df[["base_currency", "target_currency"]] = df["pair"].str.extract(r"([A-Z]{3})([A-Z]{3})")

    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    df["base_currency"] = df["base_currency"].astype(str)
    df["target_currency"] = df["target_currency"].astype(str)

    # ✅ التوقيت المحلي
    cairo_tz = pytz.timezone("Africa/Cairo")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["retrieved_at"] = datetime.now(cairo_tz)

    # ✅ ترتيب الأعمدة
    df = df[["base_currency", "target_currency", "rate", "timestamp", "retrieved_at"]].dropna()

    # ✅ تعريف الـ Schema
    schema = [
        bigquery.SchemaField("base_currency", "STRING"),
        bigquery.SchemaField("target_currency", "STRING"),
        bigquery.SchemaField("rate", "FLOAT64"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("retrieved_at", "TIMESTAMP"),
    ]

    # ✅ تأكيد وجود الجداول
    ensure_table_exists(client, historical_table, schema)
    ensure_table_exists(client, current_table, schema)
    ensure_table_exists(client, tmp_table, schema)

    # ✅ إنشاء CSV مؤقت
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp_csv:
        df.to_csv(tmp_csv.name, index=False)
        tmp_path = tmp_csv.name
    print(f"📄 Temporary CSV created at {tmp_path}")

    # ✅ تحميل البيانات إلى historical_rates
    hist_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
    )
    with open(tmp_path, "rb") as f:
        client.load_table_from_file(f, historical_table, job_config=hist_config).result()
    print("✅ Appended to historical_rates successfully!")

    # ✅ تحميل بيانات مؤقتة إلى tmp_rates
    tmp_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )
    with open(tmp_path, "rb") as f:
        client.load_table_from_file(f, tmp_table, job_config=tmp_config).result()

    # ✅ MERGE لتحديث current_rates
    merge_sql = f"""
    MERGE `{current_table}` T
    USING `{tmp_table}` S
    ON T.base_currency = S.base_currency AND T.target_currency = S.target_currency
    WHEN MATCHED AND S.timestamp > T.timestamp THEN
      UPDATE SET 
        rate = S.rate,
        timestamp = S.timestamp,
        retrieved_at = S.retrieved_at
    WHEN NOT MATCHED THEN
      INSERT (base_currency, target_currency, rate, timestamp, retrieved_at)
      VALUES (S.base_currency, S.target_currency, S.rate, S.timestamp, S.retrieved_at)
    """
    client.query(merge_sql).result()
    print("✅ current_rates table merged successfully!")

    # 🧹 تنظيف الملفات المؤقتة
    try:
        client.delete_table(tmp_table, not_found_ok=True)
        os.remove(tmp_path)
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")
    print("🧹 Temporary files and tables cleaned successfully!")

# ============================================================
# 🔹 تحميل ملف معين من GCS (Airflow يمرر الاسم)
# ============================================================
def load_from_gcs_to_bq(gcs_filename):
    """Reads a specific transformed CSV from GCS and loads it to BigQuery."""
    bucket_name = "bigdata-ai-datalake"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_filename)

    print(f"📥 Downloading file from GCS: gs://{bucket_name}/{gcs_filename}")
    if not blob.exists():
        raise FileNotFoundError(f"❌ File not found in GCS: {gcs_filename}")

    # ✅ قراءة CSV كـ DataFrame
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    print(f"✅ File downloaded successfully — {len(df)} rows")

    # ✅ تحميل إلى BigQuery
    load_to_bq(df)
    print("🎯 File loaded successfully into BigQuery!")

# ============================================================
# 🔹 Main Entry Point (للـ local testing فقط)
# ============================================================
if __name__ == "__main__":
    print("🚀 Starting Load Stage to BigQuery (manual test)...")
    test_file = "clean1/exchangerate/live/USD_transformed_20251110_140000.csv"
    try:
        load_from_gcs_to_bq(test_file)
        print("🎉 BigQuery load pipeline completed successfully (Cairo Time)!")
    except Exception as e:
        print(f"❌ Error in Load pipeline: {e}")
