import streamlit as st
import pandas as pd
import os
import time
import gc
from celery.result import AsyncResult
import shutil
from tasks import process_csv_batch, app
from analysis_sql import get_summary, get_total_summary, get_financial_dashboard_summary
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

if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = None

if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0

# -----------------------------
# 📤 UPLOAD SECTION
# -----------------------------
st.header("📤 Upload Data")

upload_type = st.selectbox("Select Upload Type", list(TEMPLATES.keys()))

uploaded_files = st.file_uploader(
    "Upload CSV files",
    type=["csv"],
    accept_multiple_files=True,
    key=f"uploader_{st.session_state.uploader_key}"
)

if uploaded_files:
    st.session_state.uploaded_files = uploaded_files

files = st.session_state.uploaded_files

if files:
    os.makedirs("uploads", exist_ok=True)
    file_paths = []

    # PREVIEW ALL FILES
    for f in uploaded_files:
        file_path = os.path.join("uploads", f.name)
        with open(file_path, "wb") as file:
            file.write(f.getbuffer())
        file_paths.append(file_path)

        with st.expander(f"👀 Preview: {f.name}", expanded=False):
            delimiter = detect_delimiter(file_path)
            df_preview = pd.read_csv(file_path, sep=delimiter, nrows=10)
            df_preview.columns = [c.strip() for c in df_preview.columns]
            st.dataframe(df_preview, width='stretch')

    # VALIDATION
    schema = TEMPLATES[upload_type]
    mapping = schema.get("column_mapping", {})

    sample_df = pd.read_csv(file_paths[0], sep=detect_delimiter(file_paths[0]), nrows=5)
    sample_df.columns = [c.strip() for c in sample_df.columns]

    mapped_columns = {col: mapping.get(col, col) for col in sample_df.columns}
    db_columns_found = list(mapped_columns.values())

    required_to_check = [
        col for col in schema["required"]
        if not (upload_type == "STMT.ENTRY" and col == "amount")
        if not (upload_type == "RE.CONSOL.SPEC.ENTRY" and col == "asset_type")
    ]

    missing_required = [col for col in required_to_check if col not in db_columns_found]

    # START BATCH BUTTON
    if missing_required:
        st.error(f"Missing required columns in CSV: {missing_required}")
    elif st.session_state.task_id is None:
        if st.button("🚀 Start Batch Upload (Sequential)"):
            st.session_state.processing_complete = False
            task = process_csv_batch.delay(
                file_paths,
                upload_type,
                truncate=True
            )
            st.session_state.task_id = task.id
            st.rerun()

# -----------------------------
# ⏳ PROGRESS TRACKING
# -----------------------------
if st.session_state.task_id:
    st.divider()
    st.header("📡 Live Task Status")
    task_id = st.session_state.task_id
    task_result = AsyncResult(task_id, app=app)
    status_box = st.empty()
    progress_bar = st.progress(0)

    with st.spinner("Processing batch files sequentially..."):
        while task_result.state in ["PENDING", "STARTED", "RETRY"]:
            status_box.info(f"Status: {task_result.state}")
            if isinstance(task_result.info, dict):
                progress = task_result.info.get("progress", 0)
                processed_count = task_result.info.get("processed", 0)
                progress_bar.progress(progress / 100)
                status_box.info(f"Status: {task_result.state} | Uploaded: {processed_count:,} records")
            time.sleep(2)
            task_result = AsyncResult(task_id, app=app)

        if task_result.state == "SUCCESS":
            progress_bar.progress(1.0)
            st.session_state.processing_complete = True
            st.session_state.task_id = None
            st.rerun()
        elif task_result.state == "FAILURE":
            st.error(f"❌ Task Failed: {task_result.info}")
            if st.button("Reset"):
                st.session_state.task_id = None
                st.rerun()

# -----------------------------
# ✅ COMPLETION MESSAGE
# -----------------------------
if st.session_state.processing_complete:
    st.divider()
    st.balloons()
    st.success("✅ Batch Upload Completed Successfully!")
    if st.button("Continue to Analytics"):
        st.session_state.uploaded_files = None
        st.session_state.uploader_key += 1
        shutil.rmtree("uploads", ignore_errors=True)
        st.session_state.processing_complete = False
        st.session_state.task_id = None
        st.rerun()

# -----------------------------
# 📊 ANALYTICS SECTION
# -----------------------------
st.divider()
st.header("📊 Multi-Table Analytics")

tab1, tab2, tab3, tab4 = st.tabs([
    "STMT.ENTRY",
    "CATEG.ENTRY",
    "RE.CONSOL.SPEC.ENTRY",
    "TOTAL ANALYTICS"
])

def render_table_analytics(table_name):
    try:
        df_check = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1", engine)
        if df_check.empty:
            st.info(f"The table '{table_name}' is empty.")
            return

        st.subheader(f"🔍 Filter {table_name}")
        f_col1, f_col2, f_col3, f_col4 = st.columns(4)
        active_filters = {}

        with f_col1:
            codes = pd.read_sql(f"SELECT DISTINCT transaction_code FROM {table_name}", engine)
            selected_codes = st.multiselect(f"Transaction Codes", codes["transaction_code"].dropna().unique(), key=f"code_{table_name}")
            if selected_codes: active_filters["transaction_code"] = selected_codes

        with f_col2:
            valdates = pd.read_sql(f"SELECT DISTINCT value_date FROM {table_name}", engine)
            selected_valdates = st.multiselect(f"Value Date", valdates["value_date"].dropna().unique(), key=f"valdate_{table_name}")
            if selected_valdates: active_filters["value_date"] = selected_valdates

        with f_col3:
            booking_dates = pd.read_sql(f"SELECT DISTINCT booking_date FROM {table_name}", engine)
            selected_booking_dates = st.multiselect(f"Booking Date", booking_dates["booking_date"].dropna().unique(), key=f"booking_{table_name}")
            if selected_booking_dates: active_filters["booking_date"] = selected_booking_dates

        with f_col4:
            try:
                cats = pd.read_sql(f"SELECT DISTINCT category FROM {table_name}", engine)
                selected_cats = st.multiselect(f"Category", cats["category"].dropna().unique(), key=f"cat_{table_name}")
                if selected_cats: active_filters["category"] = selected_cats
            except: st.caption("No category column")

        st.divider()
        summary = get_summary(table_name, active_filters)
        st.dataframe(summary, width='stretch')

    except Exception as e:
        st.error(f"Error loading {table_name}: {e}")

def render_total_table_analytics():
    try:
        st.subheader("📋 Consolidated Financial Dashboard")
        summary_df = get_financial_dashboard_summary()
        
        if not summary_df.empty:
            #m1, m2, m3, m4, m5 = st.columns(5)
            m1, m2 = st.columns(2)
            
            # Updated helper to use the underscore versions
            def get_val(tbl, col):
                return summary_df[summary_df['Table'].str.contains(tbl, case=False, na=False)][col].values[0]

            stmt_amt = get_val('Stmt', 'Total_Amount')
            spec_amt = get_val('Spec', 'Total_Amount')
            categ_amt = get_val('Categ', 'Total_Amount')
            grand_rows = get_val('GRAND', 'Row_Count')

            m1.metric("Total Records Uploaded", f"{grand_rows:,}")
            #m2.metric("Stmt Entry Balance", f"GH₵ {stmt_amt:,.2f}")
            #m3.metric("Spec Entry Balance", f"GH₵ {spec_amt:,.2f}")
            #m4.metric("Categ Entry Balance", f"GH₵ {categ_amt:,.2f}")
            m2.metric("Reconciliation Gap", f"GH₵ {stmt_amt + spec_amt + categ_amt:,.2f}")

            st.divider()
            st.write("### Data Integrity Breakdown")
            
            # Format display names for the UI by stripping underscores
            display_df = summary_df.rename(columns={
                "Row_Count": "Rows",
                "Total_Amount": "Total Amount"
            })

            st.dataframe(
                display_df.style.format({
                    "Rows": "{:,}",
                    "Total Amount": "GH₵ {:,.2f}"
                }),
                width='stretch',
                hide_index=True
            )
        else:
            st.info("No data available for total summary.")
    except Exception as e:
        st.error(f"Error in Total Analytics: {e}")


def render_total_table_analytics1():
    """
    NEW: Enhanced dashboard for final reconciliation and cross-table totals.
    """
    try:
        st.subheader("📋 Consolidated Financial Dashboard")
        summary_df = get_financial_dashboard_summary()
        
        if not summary_df.empty:
            # Metric Row
            m1, m2, m3 = st.columns(3)
            
            # Helper to safely extract values from the summary dataframe
            def get_val(tbl, col, default=0):
                filtered = summary_df[summary_df['Table'].str.contains(tbl, case=False, na=False)]
                if not filtered.empty:
                    return filtered[col].values[0]
                else:
                    return default

            stmt_amt = get_val('Stmt', 'Total Amount')
            spec_amt = get_val('Spec', 'Total Amount')
            grand_rows = get_val('GRAND', 'Rows')

            m1.metric("Total Records Uploaded", f"{grand_rows:,}")
            m2.metric("Stmt Entry Balance", f"GH₵ {stmt_amt:,.2f}")
            m3.metric("Reconciliation Gap", f"GH₵ {stmt_amt - spec_amt:,.2f}")

            st.divider()
            st.write("### Data Integrity Breakdown")
            st.dataframe(
                summary_df.style.format({
                    "Rows": "{:,}",
                    "Total Amount": "GH₵ {:,.2f}"
                }),
                width='stretch',
                hide_index=True
            )

            # Download Consolidated
            csv = summary_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Consolidated Summary", csv, "consolidated_summary.csv", "text/csv")
        else:
            st.info("No data available for total summary.")
    except Exception as e:
        st.error(f"Error in Total Analytics: {e}")

# Tab Execution
with tab1: render_table_analytics("stmt_entry")
with tab2: render_table_analytics("categ_entry")
with tab3: render_table_analytics("re_consol_spec_entry")
with tab4: render_total_table_analytics()