import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import requests
import os
from datetime import datetime
import pytz

# ============================
# 🔐 Setup
# ============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(BASE_DIR, "../keys/gcp.json")

st.set_page_config(page_title="💹 Currency ETL Dashboard", page_icon="💱", layout="wide")
st.title("💹 Exchange Rate ETL Dashboard")
st.markdown("### Live Currency Data from your Airflow → BigQuery pipeline")

# ============================
# ⚡ Auto Refresh
# ============================
st_autorefresh = st.sidebar.checkbox("🔁 Auto-refresh every minute", value=False)
if st_autorefresh:
    st.rerun()

# ============================
# 🔗 Connect to BigQuery
# ============================
try:
    client = bigquery.Client()
    dataset = "currency"
    historical_table = f"{client.project}.{dataset}.historical_rates"
    current_table = f"{client.project}.{dataset}.current_rates"
except Exception as e:
    st.error(f"❌ Couldn't connect to BigQuery. Check credentials.\n\n**Error:** {e}")
    st.stop()

# ============================
# 📊 Load Data
# ============================
@st.cache_data(ttl=600)
def load_data():
    query = f"""
        SELECT * FROM `{historical_table}`
        ORDER BY timestamp DESC
        LIMIT 5000
    """
    return client.query(query).to_dataframe()

try:
    df = load_data()
except Exception:
    df = pd.DataFrame()

# ============================
# 🎛 Filters
# ============================
st.sidebar.header("🎛 Filters")

try:
    # حاول تجيب كل العملات من الـ API
    api_symbols = requests.get("https://api.exchangerate.host/symbols", timeout=5).json()
    available_currencies = sorted(api_symbols["symbols"].keys())
except Exception:
    # 🧩 fallback list (في حالة فشل الـ API)
    available_currencies = sorted([
        "USD", "EUR", "GBP", "EGP", "SAR", "AED", "KWD", "BHD", "OMR", "QAR",
        "JPY", "CNY", "INR", "PKR", "CAD", "AUD", "NZD", "CHF", "SEK", "NOK",
        "DKK", "ZAR", "RUB", "TRY", "BRL", "MXN", "SGD", "HKD", "MYR", "THB",
        "IDR", "KRW", "PLN", "CZK", "HUF", "RON", "ILS", "CLP", "COP", "ARS",
        "NGN", "MAD", "TND", "LBP", "IQD", "SYP", "JOD", "SDG", "LYD", "DZD"
    ])

base_currency = st.sidebar.selectbox(
    "Base Currency",
    available_currencies,
    index=available_currencies.index("USD") if "USD" in available_currencies else 0
)

target_currency = st.sidebar.selectbox(
    "Target Currency",
    available_currencies,
    index=available_currencies.index("EGP") if "EGP" in available_currencies else 0
)

if base_currency == target_currency:
    st.warning("⚠️ Base and Target currencies cannot be the same.")
    st.stop()

filtered = df[(df["base_currency"] == base_currency) & (df["target_currency"] == target_currency)]

# ============================
# 📈 Charts + Current Rate
# ============================
st.subheader(f"💹 {base_currency} → {target_currency} Historical Rates")

if not filtered.empty:
    fig = px.line(
        filtered.sort_values("timestamp"),
        x="timestamp",
        y="rate",
        title=f"{base_currency} → {target_currency} Exchange Rate Over Time",
        markers=True,
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

    last_row = filtered.sort_values("timestamp").iloc[-1]
    st.metric(label=f"Latest {base_currency} → {target_currency} Rate", value=f"{last_row['rate']:.4f}")
else:
    st.warning("⚠️ No data available in BigQuery for this pair. Try running ETL below.")

# ============================
# 🧠 Trigger Airflow DAG
# ============================
st.write("---")
st.subheader("🪄 Run Airflow ETL Pipeline")

if st.button("🚀 Run ETL Pipeline Now"):
    airflow_url = "http://localhost:8082/api/v1/dags/fetch_transform_load_dag/dagRuns"
    payload = {
        "conf": {
            "base_currency": base_currency,
            "target_currency": target_currency,
            "triggered_by": "streamlit"
        },
        "note": f"Triggered from Streamlit ({base_currency} → {target_currency})"
    }

    try:
        response = requests.post(
            airflow_url,
            auth=("airflow", "airflow"),
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.status_code in [200, 201]:
            st.success(f"✅ DAG triggered successfully for {base_currency} → {target_currency}! Check Airflow UI.")
        else:
            st.error(f"❌ Failed to trigger DAG.\n\n**Response:** {response.text}")
    except Exception as e:
        st.error(f"⚠️ Couldn't reach Airflow API.\n\n**Error:** {e}")

# ============================
# 📊 Show Latest Stored Rate in BigQuery
# ============================
st.write("---")
st.subheader("📊 Latest Stored Rate from BigQuery")

def get_latest_bq_rate(base, target):
    try:
        query = f"""
            SELECT rate, timestamp
            FROM `{current_table}`
            WHERE base_currency = '{base}' AND target_currency = '{target}'
            ORDER BY timestamp DESC
            LIMIT 1
        """
        df_latest = client.query(query).to_dataframe()
        if df_latest.empty:
            return None, None
        return df_latest["rate"].iloc[0], df_latest["timestamp"].iloc[0]
    except Exception as e:
        st.error(f"❌ BigQuery Error: {e}")
        return None, None

if st.button("📊 Show Latest Rate from BigQuery"):
    rate, ts = get_latest_bq_rate(base_currency, target_currency)
    if rate:
        st.success(
            f"✅ Latest Stored Rate:\n\n"
            f"**1 {base_currency} = {rate:.4f} {target_currency}**\n\n"
            f"🕒 Stored at: **{ts}**"
        )
    else:
        st.warning("⚠️ No stored rate found in BigQuery for this currency pair.")

# ============================
# 🕒 Footer
# ============================
st.write("---")
st.caption(
    f"📅 Last updated: {datetime.now(pytz.timezone('Africa/Cairo')).strftime('%Y-%m-%d %H:%M:%S')} | "
    f"👨‍💻 Built with ❤️ by Abdo"
)
