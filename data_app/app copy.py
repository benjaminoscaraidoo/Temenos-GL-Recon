import streamlit as st
import pandas as pd
import os
import time
from celery.result import AsyncResult
# --- app.py initialization ---
from celery import Celery

# Make sure this matches your tasks.py configuration exactly

from tasks import process_csv
from schemas import TEMPLATES
from utils.detect_delimiter import detect_delimiter
from analysis_sql import get_summary
from db import get_engine

celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0" # This is the missing piece!
)

st.set_page_config(page_title="GL Analytics Platform", layout="wide")
engine = get_engine()
st.title("📊 GL Analytics Platform")

# --- Initialize Session State ---
if "batch_tasks" not in st.session_state:
    st.session_state.batch_tasks = []  # List of dicts with file info and task IDs
if "all_done" not in st.session_state:
    st.session_state.all_done = False

# --- Upload Section ---
upload_type = st.selectbox("Select Template Type", list(TEMPLATES.keys()))
uploaded_files = st.file_uploader("Upload CSV files", type=["csv"], accept_multiple_files=True)

if uploaded_files:
    os.makedirs("uploads", exist_ok=True)
    
    # Preview Section
    for f in uploaded_files:
        path = os.path.join("uploads", f.name)
        with open(path, "wb") as out:
            out.write(f.getbuffer())
        
        with st.expander(f"👀 Preview: {f.name}"):
            df_preview = pd.read_csv(path, sep=detect_delimiter(path), nrows=5)
            st.dataframe(df_preview)

    # Start Button
    if st.button("🚀 Start All Uploads"):
        st.session_state.all_done = False
        task_list = []
        truncate = True  # Truncate only on the first file of the batch
        
        for f in uploaded_files:
            path = os.path.join("uploads", f.name)
            # Dispatch Celery Task
            task = process_csv.delay(path, upload_type, truncate=truncate)
            task_list.append({
                "name": f.name,
                "task_id": task.id,
                "total_rows": sum(1 for _ in open(path, 'r', encoding='utf-8', errors='ignore')) - 1
            })
            truncate = False # Don't truncate subsequent files in the same batch
            
        st.session_state.batch_tasks = task_list
        st.rerun()

# --- Progress Dashboard ---
if st.session_state.batch_tasks:
    st.divider()
    st.header("⏳ Upload Progress")
    
    overall_progress_bar = st.progress(0)
    overall_status_text = st.empty()
    
    total_rows_all_files = sum(t['total_rows'] for t in st.session_state.batch_tasks)
    processed_rows_all_files = 0
    tasks_running = False

    # Create a grid for individual file status
    cols = st.columns(len(st.session_state.batch_tasks))
    
    for i, task_info in enumerate(st.session_state.batch_tasks):
        result = AsyncResult(task_info['task_id'], app=celery_app)
        
        with cols[i]:
            st.caption(f"📄 {task_info['name']}")
            
            if result.state == 'PROGRESS':
                tasks_running = True
                progress_data = result.info.get('progress', 0)
                # Calculate rows based on percentage for the aggregate count
                processed_rows_all_files += (task_info['total_rows'] * (progress_data / 100))
                st.info(f"Processing... {progress_data}%")
            
            elif result.state == 'SUCCESS':
                processed_rows_all_files += task_info['total_rows']
                st.success("Completed")
            
            elif result.state == 'FAILURE':
                st.error("Failed")
            
            else:
                tasks_running = True
                st.warning("Queued...")

    # Calculate and display Aggregate Progress
    total_percent = int((processed_rows_all_files / total_rows_all_files) * 100) if total_rows_all_files > 0 else 0
    overall_progress_bar.progress(total_percent / 100)
    overall_status_text.markdown(f"**Total Progress:** {total_percent}% ({int(processed_rows_all_files):,} / {total_rows_all_files:,} total rows)")

    # Auto-refresh loop
    if tasks_running:
        time.sleep(1)
        st.rerun()
    else:
        st.session_state.all_done = True
        st.session_state.batch_tasks = [] # Clear tasks list
        st.rerun()

# --- Completion Popup ---
if st.session_state.all_done:
    st.balloons()
    st.success("✅ COMPLETED: All files have been uploaded and processed successfully!")
    if st.button("Dismiss"):
        st.session_state.all_done = False
        st.rerun()

# --- Analytics Section (remains the same but simplified for space) ---
# ... (Your existing render_table_analytics code here) ...

# -----------------------------
# Analytics section (same as before)
# -----------------------------
st.divider()
st.header("📊 Multi-Table Analytics")
tab1, tab2, tab3 = st.tabs(["STMT.ENTRY", "CATEG.ENTRY", "RE.CONSOL.SPEC.ENTRY"])

def render_table_analytics(table_name):
    try:
        df_check = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1", engine)
        if df_check.empty:
            st.info(f"The table '{table_name}' is empty. Upload data to see analytics.")
            return

        st.subheader(f"🔍 Filter {table_name}")
        f_col1, f_col2, f_col3, f_col4 = st.columns(4)
        active_filters = {}

        with f_col1:
            codes = pd.read_sql(f"SELECT DISTINCT transaction_code FROM {table_name}", engine)
            selected_codes = st.multiselect(f"Transaction Codes ({table_name})", codes["transaction_code"].unique(), key=f"code_{table_name}")
            if selected_codes: active_filters["transaction_code"] = selected_codes
        with f_col2:
            valdates = pd.read_sql(f"SELECT DISTINCT value_date FROM {table_name}", engine)
            selected_valdates = st.multiselect(f"Value Date ({table_name})", valdates["value_date"].unique(), key=f"valdate_{table_name}")
            if selected_valdates: active_filters["value_date"] = selected_valdates
        with f_col3:
            booking_dates = pd.read_sql(f"SELECT DISTINCT booking_date FROM {table_name}", engine)
            selected_booking_dates = st.multiselect(f"Booking Date ({table_name})", booking_dates["booking_date"].unique(), key=f"booking_date_{table_name}")
            if selected_booking_dates: active_filters["booking_date"] = selected_booking_dates
        with f_col4:
            try:
                cats = pd.read_sql(f"SELECT DISTINCT category FROM {table_name}", engine)
                selected_cats = st.multiselect(f"Categories ({table_name})", cats["category"].unique(), key=f"cat_{table_name}")
                if selected_cats: active_filters["category"] = selected_cats
            except:
                st.caption("No category column found for this table.")

        st.divider()
        summary = get_summary(table_name, active_filters)
        st.dataframe(summary, width='stretch')
        csv = summary.to_csv(index=False).encode('utf-8')
        st.download_button(f"📥 Download {table_name} Report", data=csv, file_name=f"{table_name}_analytics.csv", mime="text/csv", key=f"dl_{table_name}")

    except Exception as e:
        st.error(f"Error loading {table_name}: {e}")

with tab1: render_table_analytics("stmt_entry")
with tab2: render_table_analytics("categ_entry")
with tab3: render_table_analytics("re_consol_spec_entry")