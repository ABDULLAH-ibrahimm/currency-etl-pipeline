# 💹 Currency ETL Pipeline (Airflow + Streamlit + GCP)

An end-to-end **Data Engineering project** for automating currency exchange rate ingestion, transformation, and analysis — fully orchestrated with **Apache Airflow** inside Docker, and visualized via **Streamlit Dashboard**.

---

## 🏗️ Architecture Overview

### 🔸 Workflow Diagram
![Workflow](Images/etl_workflow_diagram.png)

### 🔹 Components
- **Streamlit App** — User interface to trigger ETL and visualize data.
- **Apache Airflow (Docker)** — Orchestrates the ETL workflow.
- **Google Cloud Storage (GCS)** — Stores raw and cleaned CSVs.
- **BigQuery** — Stores historical and current exchange rate tables.
- **Email Notification** — Sends status updates after each pipeline run.

---

## ⚙️ Project Structure
```bash
currency-etl-pipeline/
├── dags/
│   ├── fetch_transform_load_dag.py
│   └── scripts/
│       ├── fetch.py
│       ├── transform.py
│       └── load_to_bigquery.py
├── app/
│   └── app.py
├── docker/
│   ├── docker-compose.yml
│   └── Dockerfile
├── images/
│   └── etl_workflow_diagram.png
├── requirements.txt
└── README.md
```

---

## 🚀 How to Run Locally

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/your-username/currency-etl-pipeline.git
cd currency-etl-pipeline
```

### 2️⃣ Setup Google Cloud Credentials

* Create a **Service Account** in GCP with access to GCS + BigQuery.
* Download `gcp.json` key and place it under `/keys/gcp.json`.
* Update the file path in scripts if needed.

### 3️⃣ Start Airflow in Docker

```bash
cd docker
docker-compose up -d
```

Once running:

* Airflow UI → `http://localhost:8082`
* Default creds: `airflow / airflow`

### 4️⃣ Run Streamlit Dashboard

```bash
cd app
streamlit run app.py
```

Dashboard available at `http://localhost:8501`

---

## 🪄 DAG Overview

| Stage         | Script                | Description                              |
| ------------- | --------------------- | ---------------------------------------- |
| **Fetch**     | `fetch.py`            | Fetches live exchange rate data from API |
| **Transform** | `transform.py`        | Cleans and prepares data for loading     |
| **Load**      | `load_to_bigquery.py` | Loads data into BigQuery tables          |
| **Notify**    | `EmailOperator`       | Sends summary report to team             |

---

## 🧠 BigQuery Schema

| Column          | Type      | Description                |
| --------------- | --------- | -------------------------- |
| base_currency   | STRING    | Base currency code         |
| target_currency | STRING    | Target currency code       |
| rate            | FLOAT     | Exchange rate              |
| timestamp       | TIMESTAMP | Time when rate was fetched |
| retrieved_at    | TIMESTAMP | Time of ETL insertion      |

---

## 📊 Streamlit Dashboard Features

* ✅ Select **Base** and **Target** currencies dynamically
* 📈 View **Historical exchange trends**
* 🚀 Trigger **ETL DAG** directly via Airflow REST API
* 🗂️ Display **latest stored rate** from BigQuery
* 🔁 Auto-refresh option every minute

---

## 📧 Notifications

* Pipeline completion emails are sent to:

  * `abdulllah02003@gmail.com`
  * `ahmed.azab201829@gmail.com`
* Each email includes:

  * Latest exchange rate (e.g. `1 USD = 15.65 EGP`)
  * Rate change percentage over the last 24 hours
  * Summary of ETL stages (Fetch → Transform → Load → Notify)

---

## 🧩 Technologies Used

* 🐍 Python 3.8+
* 🪶 Apache Airflow
* 🐳 Docker Compose
* ☁️ Google Cloud Storage (GCS)
* 📊 Google BigQuery
* 💻 Streamlit
* 📈 Plotly Express

---


## 🪪 License

This project is licensed under the **MIT License** — feel free to reuse and modify.

---


👨‍💻 *Built with Abdullah ❤️ using Airflow, Streamlit, and GCP*

