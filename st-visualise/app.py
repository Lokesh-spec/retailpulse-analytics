import streamlit as st
import pandas as pd
from data.bigquery_client import get_bq_client

# -----------------------------
# Setup
# -----------------------------
st.set_page_config(page_title="RetailPulse Analytics", layout="wide")

client = get_bq_client()

# -----------------------------
# Data Loading
# -----------------------------
@st.cache_data(ttl=3600)
def get_retail_data():
    query = """
        SELECT f.*, d.product_name, d.product_category, d.product_brand
        FROM `retailpulse-analytics.retailpulse_marts.fct_inventory` f
        LEFT JOIN `retailpulse-analytics.retailpulse_marts.dim_products` d
        ON f.product_id = d.product_id
    """
    return client.query(query).to_dataframe()

df = get_retail_data()

# -----------------------------
# Data Cleaning
# -----------------------------
df = df.copy()

df["lead_time_days"] = pd.to_numeric(df["lead_time_days"], errors="coerce")
df["stock_quantity"] = pd.to_numeric(df["stock_quantity"], errors="coerce")

df = df.dropna(subset=["supplier_name", "product_name"])

# -----------------------------
# Global Filters
# -----------------------------
st.sidebar.header("Filters")

supplier_filter = st.sidebar.selectbox(
    "Supplier",
    ["All"] + sorted(df["supplier_name"].unique().tolist())
)

if supplier_filter != "All":
    df = df[df["supplier_name"] == supplier_filter]

# -----------------------------
# Tabs
# -----------------------------
tab1, tab2, tab3 = st.tabs([
    "Inventory Overview",
    "Supplier Analysis",
    "Demand vs Inventory"
])

# =========================================================
# Page 1 — Inventory Overview
# =========================================================
with tab1:
    st.header("Inventory Overview")

    col1, col2, col3 = st.columns(3)

    total_products = df["product_name"].nunique()
    low_stock = df[df["alert_flag"] == True].shape[0]
    high_risk = df[df["inventory_risk_flag"] == "HIGH_RISK"].shape[0]

    col1.metric("Total Products", total_products)
    col2.metric("Low Stock Count", low_stock)
    col3.metric("High Risk Count", high_risk)

    st.subheader("Stock Status Distribution")
    status_dist = df["normalized_stock_status"].value_counts()
    st.bar_chart(status_dist)

    st.subheader("Inventory by Region")
    region_dist = df.groupby("warehouse_region")["stock_quantity"].sum()
    st.bar_chart(region_dist)

    # Extra Insight
    st.subheader("Top 10 High Risk Products")
    high_risk_df = df[df["inventory_risk_flag"] == "HIGH_RISK"]

    top_risk = (
        high_risk_df.groupby("product_name")["stock_quantity"]
        .sum()
        .sort_values()
        .head(10)
    )

    st.dataframe(top_risk)

# =========================================================
# Page 2 — Supplier Analysis
# =========================================================
with tab2:
    st.header("Supplier Analysis")

    col1, col2 = st.columns(2)

    avg_lead_time = df["lead_time_days"].mean()
    high_risk_suppliers = df[df["inventory_risk_flag"] == "HIGH_RISK"]["supplier_name"].nunique()

    col1.metric("Average Lead Time", f"{avg_lead_time:.2f}")
    col2.metric("High Risk Suppliers", high_risk_suppliers)

    supplier_df = (
        df.groupby("supplier_name")
        .agg(
            avg_lead_time=("lead_time_days", "mean"),
            total_stock=("stock_quantity", "sum")
        )
        .reset_index()
    )

    st.subheader("Supplier vs Lead Time")
    st.bar_chart(
        supplier_df.set_index("supplier_name")["avg_lead_time"]
    )

    st.subheader("Supplier vs Stock")
    st.bar_chart(
        supplier_df.set_index("supplier_name")["total_stock"]
    )

    # Extra Insight
    st.subheader("Suppliers with Highest Risk")

    risk_suppliers = (
        df[df["inventory_risk_flag"] == "HIGH_RISK"]
        .groupby("supplier_name")
        .size()
        .sort_values(ascending=False)
    )

    st.dataframe(risk_suppliers)

# =========================================================
# Page 3 — Demand vs Inventory
# =========================================================
with tab3:
    st.header("Demand vs Inventory")

    # Using only flags available
    st.subheader("High Demand Products")

    high_demand_df = df[df["high_demand_flag"] == True]

    st.dataframe(
        high_demand_df[
            ["product_name", "stock_quantity", "inventory_risk_flag"]
        ]
    )

    st.subheader("Stock vs Risk Distribution")

    risk_dist = df.groupby("inventory_risk_flag")["stock_quantity"].sum()
    st.bar_chart(risk_dist)

    # Critical Insight
    st.subheader("High Demand + Low Stock (Critical)")

    critical_df = df[
        (df["high_demand_flag"] == True) &
        (df["alert_flag"] == True)
    ]

    st.dataframe(
        critical_df[
            ["product_name", "stock_quantity", "inventory_risk_flag"]
        ]
    )