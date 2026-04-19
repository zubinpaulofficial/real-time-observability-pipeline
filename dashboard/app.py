import streamlit as st
import requests
import pandas as pd

st.title("Observability Dashboard")

API_URL = "http://api:8000/metrics"

response = requests.get(API_URL)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)

    st.subheader("Service Metrics")
    st.dataframe(df)

    st.subheader("Average Latency")
    st.bar_chart(df.set_index("service")["avg_latency"])

    st.subheader("Error Count")
    st.bar_chart(df.set_index("service")["errors"])

    st.subheader("Anomaly Count")
    st.metric("Total Anomalies", df["errors"].sum())

else:
    st.error("Failed to fetch data")