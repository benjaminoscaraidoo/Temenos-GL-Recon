import streamlit as st
import pandas as pd
import os
import time
from celery.result import AsyncResult

from tasks import process_csv,app
from analysis_sql import get_summary
from db import get_engine
from utils.detect_delimiter import detect_delimiter
from schemas import TEMPLATES

# Page Config
st.set_page_config(page_title="GL Analytics Platform", layout="wide")
engine = get_engine()

st.title("📊 GL Analytics Platform")

# -----------------------------
# SESSION STATE INIT
# -----------------------------
if "task_id" not in st.session_state:
    st.session_state.task_id = None
if "processing_complete" not in st.session_state:
    st.session_state.processing_complete = False

# -----------------------------
# 📤 UPLOAD SECTION
# -----------------------------
st.header("📤 Upload Data")

upload_type = st.selectbox("Select Upload Type", [
    "STMT.ENTRY",
    "CATEG.ENTRY",
    "RE.CONSOL.SPEC.ENTRY"
])

uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

# This block stays visible as long as a file is in the uploader
if uploaded_file:
    # Save file locally for processing
    os.makedirs("uploads", exist_ok=True)
    file_path = os.path.join("uploads", uploaded_file.name)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    # 1. PREVIEW LOGIC (Placed outside buttons so it's "Sticky")
    delimiter = detect_delimiter(file_path)
    # We load a small chunk for the UI preview
    df_preview = pd.read_csv(file_path, sep=delimiter, nrows=10)
    df_preview.columns = [c.strip() for c in df_preview.columns]

    # Show preview in an expander so it doesn't crowd the dashboard
    with st.expander("👀 View Uploaded File Preview", expanded=True):
        st.dataframe(df_preview, use_container_width=True)

    # 2. MAPPING & VALIDATION
    schema = TEMPLATES[upload_type]
    mapping = schema.get("column_mapping", {})
    mapped_columns = {col: mapping.get(col, col) for col in df_preview.columns}
    
    db_columns_found = list(mapped_columns.values())
    required_to_check = [
        col for col in schema["required"] 
        if not (upload_type == "STMT.ENTRY" and col == "amount")
    ]
    missing_required = [col for col in required_to_check if col not in db_columns_found]

    # 3. ACTION BUTTON
    if missing_required:
        st.error(f"Missing required columns in CSV: {missing_required}")
    elif st.session_state.task_id is None:
        # Only show the "Start" button if a job isn't already running
        if st.button("🚀 Confirm & Start Processing (Truncate & Load)"):
            st.session_state.processing_complete = False
            task = process_csv.delay(file_path, upload_type)
            st.session_state.task_id = task.id
            st.rerun()

# -----------------------------
# ⏳ LOADING & STATUS (Polling)
# -----------------------------
if st.session_state.task_id:
    st.divider()
    st.header("📡 Live Task Status")
    task_id = st.session_state.task_id
    
    status_container = st.empty()
    
    with st.spinner("Executing Truncate and Batch Loading..."):
        # Check Celery Result Backend
        task_result = app.AsyncResult(task_id)

        while task_result.state in ['PENDING', 'STARTED', 'RETRY']:
            status_container.info(f"Current Status: **{task_result.state}**")
            time.sleep(2)
            task_result = app.AsyncResult(task_id)
        if task_result.state == 'SUCCESS':
            st.session_state.processing_complete = True
            st.session_state.task_id = None
            st.rerun()
        elif task_result.state == 'FAILURE':
            st.error(f"❌ Task Failed: {task_result.info}")
            if st.button("Reset Dashboard"):
                st.session_state.task_id = None
                st.rerun()

# -----------------------------
# ✅ PERSISTENT COMPLETION MESSAGE
# -----------------------------
if st.session_state.processing_complete:
    st.divider()
    st.balloons()
    st.success("✅ Loading Completed! The table was truncated and all records have been written.")
    if st.button("Finish & View Analytics"):
        st.session_state.processing_complete = False
        st.rerun()

# -----------------------------
# 📊 ANALYTICS SECTION
# -----------------------------
st.divider()
st.header("📊 Data Analytics")

#table_choice = st.selectbox("Select Table", ["stmt_entry", "categ_entry", "re_consol_spec_entry"])

analyis_options = st.selectbox("Select Analysis Type", [
    "Summary Statistics",
    "Data Quality Checks",
    "Custom SQL Query"
])

table_choice_stmt = "stmt_entry"
table_choice_categ = "categ_entry"
table_choice_recon = "re_consol_spec_entry"

try:
    # Always show current count/summary if data exists
    summary = get_summary(table_choice_stmt, {})
    if not summary.empty:
        st.subheader(f"Data Summary: {"STMT.ENTRY"}")
        st.dataframe(summary, width='stretch')
    else:
        st.info("Selected table is currently empty.")
except Exception:
    st.info("Upload data to see analytics.")


try:
    # Always show current count/summary if data exists
    summary = get_summary(table_choice_categ, {})
    if not summary.empty:
        st.subheader(f"Data Summary: {"CATEG.ENTRY"}")
        st.dataframe(summary, width='stretch')
    else:
        st.info("Selected table is currently empty.")
except Exception:
    st.info("Upload data to see analytics.")


try:
    # Always show current count/summary if data exists
    summary = get_summary(table_choice_recon, {})
    if not summary.empty:
        st.subheader(f"Data Summary: {"RE.CONSOL.SPEC.ENTRY"}")
        st.dataframe(summary, width='stretch')
    else:
        st.info("Selected table is currently empty.")
except Exception:
    st.info("Upload data to see analytics.")