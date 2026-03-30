import streamlit as st
from data.bigquery_client import get_bq_client


client = get_bq_client()

@st.cache_data
def get_retail_data(ttl=3600):
    q = """
        SELECT *
          FROM `retailpulse-analytics.retailpulse_marts.fct_inventory`
          LEFT JOIN `retailpulse-analytics.retailpulse_marts.dim_products`
         USING (product_id)
    """

    query_job = client.query(q)
    return query_job.to_dataframe()


# print(get_retail_data())