from celery import Celery
import pandas as pd
import time
import os
from sqlalchemy import text # Import this to handle raw SQL
from db import get_engine


from utils.detect_delimiter import detect_delimiter
from loader_fast import copy_chunk_to_postgres
from schemas import TEMPLATES
from progress_server import push_progress

app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

# Optional: Ensure results aren't kept forever to save memory
app.conf.update(
    result_expires=3600, # Results deleted after 1 hour
)

@app.task(bind=True)
def process_csv(self, filepath, upload_type,truncate=False):
    # 1. SETUP & CONFIGURATION
    if upload_type not in TEMPLATES:
        raise ValueError(f"Invalid upload type: {upload_type}")
        
    schema = TEMPLATES[upload_type]
    table_name = schema["table"]
    transform_func = schema.get("transform")
    engine = get_engine()

    # =====================================================
    # 💥 TRUNCATE TABLE (Run once before the loop)
    # =====================================================

    if truncate:
        try:
            with engine.connect() as conn:
                # We use execution_options(autocommit=True) to ensure it applies immediately
                conn.execute(text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;'))
                conn.commit()
                print(f"DEBUG: Successfully truncated {table_name}")
        except Exception as e:
            print(f"ERROR: Truncate failed: {e}")
            # Decide if you want to stop the task here or keep going
            raise e
    

    # Progress tracking variables
    chunk_size = 100000
    # Quick row count (consider skipping for multi-GB files to save time)
    total_rows = sum(1 for _ in open(filepath, 'rb')) - 1 
    processed = 0
    start_time = time.time()

    delimiter = detect_delimiter(filepath)

    # 2. STREAM FILE
    # Using 'on_bad_lines' as a callable can let you log skipped data
    for chunk in pd.read_csv(
        filepath,
        chunksize=chunk_size,
        sep=delimiter,
        on_bad_lines="skip", 
        engine="c",  # Faster C engine
        low_memory=False
    ):
        # A. CLEAN HEADERS
       # chunk.columns = chunk.columns.str.strip()
    # =====================================================
        # A. NORMALIZE HEADERS (Crucial for STMT.ENTRY math)
        # =====================================================
        # Force everything to lowercase to handle "DR.AMOUNT" or "Dr.Amount"
    
        chunk.columns = chunk.columns.str.strip()
        #.str.lower()



        # B. MAP COLUMNS (CSV Name -> Internal Name)
        mapping = schema.get("column_mapping", {})
        #safe_mapping = {k.strip().lower(): v for k, v in mapping.items()}
        #chunk = chunk.rename(columns=safe_mapping)

        chunk = chunk.rename(columns=mapping)


        # C. DYNAMIC TRANSFORM (Logic-driven columns like 'amount')
        if transform_func:
            chunk = transform_func(chunk)
        else:
            print(f"WARNING: No transformation function found for {upload_type}")
        db_required = schema["required"]
        db_optional = schema.get("optional", [])
        db_columns = db_required + db_optional


        # D. POST-TRANSFORM VALIDATION
        # Ensure all required DB columns now exist (including calculated ones)


        print(f"DEBUG: Processing {upload_type}")
        print(f"DEBUG: Required by Schema: {db_required}")
        print(f"DEBUG: Columns currently in chunk: {chunk.columns.tolist()}")
        
        missing = [c for c in db_required if c not in chunk.columns]
        if missing:
            # We fail the task if the transformation didn't produce a required column
            print(f"DEBUG: Failed Columns -> {chunk.columns.tolist()}")
            raise ValueError(f"CRITICAL: Missiiing columns {missing} {chunk.columns.tolist()} after {upload_type} transformation.")

        # E. STRICT ALIGNMENT & ORDERING
        # reindex ensures columns are in the exact order the DB expects for COPY
        # and fills missing optional columns with NaN.
        chunk = chunk.reindex(columns=db_columns)

        # F. FAST LOAD TO POSTGRES
        copy_chunk_to_postgres(chunk, table_name)

        # G. PROGRESS TRACKING
        processed += len(chunk)
        elapsed = time.time() - start_time
        speed = int(processed / elapsed) if elapsed > 0 else 0
        progress_pct = int((processed / total_rows) * 100) if total_rows > 0 else 0

        push_progress(self.request.id, {
            "processed": processed,
            "total": total_rows,
            "progress": progress_pct,
            "speed": speed,
            "status": "processing"
        })

    # 3. FINALIZATION
    push_progress(self.request.id, {
        "status": "done",
        "progress": 100,
        "processed": processed
    })

    return f"PROCESSED: {processed} rows in {time.time() - start_time:.2f}s"