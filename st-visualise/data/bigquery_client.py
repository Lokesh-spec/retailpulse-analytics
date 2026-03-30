import streamlit as st
from google.cloud import bigquery
import pandas as pd


@st.cache_resource
def get_bq_client():
    return bigquery.Client()

