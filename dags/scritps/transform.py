from google.cloud import storage
import pandas as pd
import io
from datetime import datetime
import os
import pytz

# ✅ إعداد بيانات Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/keys/gcp.json"


def get_latest_raw_file(bucket_name="bigdata-ai-datalake", prefix="raw1/exchangerate/live/"):
    """Fetch the latest CSV file path from GCS based on updated time."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        raise FileNotFoundError(f"❌ No files found in {prefix}")

    # ترتيب الملفات حسب تاريخ التحديث
    blobs.sort(key=lambda b: b.updated, reverse=True)
    latest_blob = blobs[0]
    print(f"🕒 Latest file detected: {latest_blob.name}")
    return latest_blob.name


def transform_from_gcs_to_df(gcs_filename):
    """Reads raw CSV from GCS, cleans it, adds Cairo timestamp."""
    print(f"📥 Downloading from GCS: {gcs_filename}")
    storage_client = storage.Client()
    bucket = storage_client.bucket("bigdata-ai-datalake")
    blob = bucket.blob(gcs_filename)

    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))

    # ✅ تنظيف الداتا
    df.dropna(subset=["rate"], inplace=True)
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    df = df.dropna(subset=["rate"])

    # ✅ التوقيت المحلي
    cairo_tz = pytz.timezone("Africa/Cairo")
    now = datetime.now(cairo_tz)
    df["processed_at"] = now.strftime("%Y-%m-%d %H:%M:%S")

    print("✅ Transformed data preview:")
    print(df.head())
    return df


def upload_transformed_to_gcs(df, output_path):
    """Uploads transformed DataFrame to GCS."""
    print(f"📤 Uploading transformed file to: {output_path}")
    storage_client = storage.Client()
    bucket = storage_client.bucket("bigdata-ai-datalake")

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    blob = bucket.blob(output_path)
    blob.upload_from_string(buffer.getvalue(), content_type="text/csv")
    print(f"✅ Uploaded transformed file to GCS: gs://bigdata-ai-datalake/{output_path}")


if __name__ == "__main__":
    print("🚀 Starting transformation process...")

    # ✅ جلب أحدث ملف خام
    input_file = get_latest_raw_file()

    # استخراج اسم العملة من اسم الملف (اختياري)
    base_name = input_file.split("/")[-1].split("_")[1] if "_" in input_file else "unknown"

    # ✅ اسم الإخراج
    cairo_tz = pytz.timezone("Africa/Cairo")
    output_file = f"clean1/exchangerate/live/{base_name}_transformed_{datetime.now(cairo_tz).strftime('%Y%m%d_%H%M%S')}.csv"

    df = transform_from_gcs_to_df(input_file)
    upload_transformed_to_gcs(df, output_file)

    print("🎉 Transformation pipeline completed successfully!")
