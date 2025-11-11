import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage
import os
import tempfile
import pytz  # ✅ لإدارة التوقيت المحلي

# ✅ إعداد بيانات Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/keys/gcp.json"


def fetch_currency_data(base_currency="GBP", target_currency=None):
    """
    Fetches live currency data using the exchangerate.host 'live' endpoint.
    Compatible with Airflow & Streamlit triggers.
    """

    # ===============================
    # 🌍 Fetch live data
    # ===============================
    url = "http://api.exchangerate.host/live"
    params = {
        "access_key": "3b83732501b180e88222d28c0b92c10a",
        "source": base_currency,
        "format": 1
    }

    print(f"🌍 Fetching data for: {base_currency} → {target_currency or 'ALL'}")
    response = requests.get(url, params=params)
    data = response.json()

    if not data.get("success"):
        raise Exception(f"❌ API Error: {data.get('error', {})}")

    # ===============================
    # 📊 Convert to DataFrame
    # ===============================
    quotes = data.get("quotes", {})
    df = pd.DataFrame(list(quotes.items()), columns=["pair", "rate"])
    df["base_currency"] = data.get("source")

    # استخراج target_currency من الزوج (مثلاً GBPUSD → USD)
    df["target_currency"] = df["pair"].str[len(base_currency):]

    # لو المستخدم حدد target_currency، صفّي عليها فقط
    if target_currency:
        df = df[df["target_currency"] == target_currency]

    # ✅ تسجيل الوقت المحلي (توقيت القاهرة)
    cairo_tz = pytz.timezone("Africa/Cairo")
    now = datetime.now(cairo_tz)
    df["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")

    if df.empty:
        raise Exception("⚠️ No rates found for the selected currencies.")

    print(f"✅ Retrieved {len(df)} rates for base {base_currency}")

    # ===============================
    # 💾 Save locally
    # ===============================
    tmp_dir = tempfile.gettempdir()
    filename = f"currency_live_{base_currency}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    local_file = os.path.join(tmp_dir, filename)
    df.to_csv(local_file, index=False)
    print(f"💾 Saved locally: {local_file}")

    # ===============================
    # ☁️ Upload to GCS
    # ===============================
    client = storage.Client()
    bucket = client.bucket("bigdata-ai-datalake")
    gcs_path = f"raw1/exchangerate/live/{filename}"
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_file)

    print(f"✅ Uploaded to GCS: gs://bigdata-ai-datalake/{gcs_path}")
    print("📊 Sample:")
    print(df.head(5))

    # ===============================
    # 🔙 Return file path (for Airflow XCom)
    # ===============================
    return gcs_path


if __name__ == "__main__":
    print("🚀 Manual test: fetching currency data ...")
    result = fetch_currency_data("USD", "EGP")
    print(f"🎉 Fetch stage completed! Uploaded file: {result}")
