from celery import Celery
import pandas as pd
import time
import os
import gc  # Added for memory management
from sqlalchemy import text
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

app.conf.update(
    result_expires=3600,
)

@app.task(bind=True)
def process_csv(self, filepath, upload_type, truncate=False):
    if upload_type not in TEMPLATES:
        raise ValueError(f"Invalid upload type: {upload_type}")
        
    schema = TEMPLATES[upload_type]
    table_name = schema["table"]
    transform_func = schema.get("transform")
    engine = get_engine()

    # 1. TRUNCATE TABLE
    if truncate:
        try:
            with engine.connect() as conn:
                conn.execute(text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;'))
                conn.commit()
                print(f"DEBUG: Successfully truncated {table_name}")
        except Exception as e:
            print(f"ERROR: Truncate failed: {e}")
            raise e

    # 2. CONFIGURATION
    # Lowered to 50k for better stability with 150MB+ files on Windows
    chunk_size = 50000 
    total_rows = sum(1 for _ in open(filepath, 'rb')) - 1 
    processed = 0
    start_time = time.time()
    delimiter = detect_delimiter(filepath)

    # 3. STREAM FILE
    try:
        for chunk in pd.read_csv(
            filepath,
            chunksize=100000, # Increased for fewer commits
            sep=delimiter,
            on_bad_lines="warn", 
            engine="python",   # Switch to 'python' engine for better stability with messy files
            low_memory=False,
            encoding_errors="replace",
            quoting=3          # csv.QUOTE_NONE: prevents it from getting stuck on open quotes
            ):
            chunk.columns = chunk.columns.str.strip()
            
            mapping = schema.get("column_mapping", {})
            chunk = chunk.rename(columns=mapping)

            if transform_func:
                chunk = transform_func(chunk)
            
            db_required = schema["required"]
            db_optional = schema.get("optional", [])
            db_columns = db_required + db_optional

            # Validation
            missing = [c for c in db_required if c not in chunk.columns]
            if missing:
                raise ValueError(f"Missing columns {missing} in {upload_type}")

            # Align & Load
            chunk = chunk.reindex(columns=db_columns)
            copy_chunk_to_postgres(chunk, table_name)

            processed += len(chunk)
            
            # Progress Update
            elapsed = time.time() - start_time
            progress_pct = int((processed / total_rows) * 100) if total_rows > 0 else 0
            push_progress(self.request.id, {
                "processed": processed,
                "total": total_rows,
                "progress": progress_pct,
                "status": "processing"
            })

            # MEMORY MANAGEMENT: Clear the chunk from RAM before next loop
            del chunk
            gc.collect()

    except Exception as e:
        print(f"CRITICAL ERROR in process_csv: {e}")
        raise e

    # 4. FINALIZATION & VERIFICATION
    if processed < (total_rows * 0.99):
        print(f"WARNING: Potential data loss. Processed {processed}/{total_rows}")

    push_progress(self.request.id, {"status": "done", "progress": 100, "processed": processed})
    return f"PROCESSED: {processed} rows"


@app.task(bind=True)
def process_csv_batch(self, filepaths, upload_type, truncate=True):
    if upload_type not in TEMPLATES:
        raise ValueError(f"Invalid upload type: {upload_type}")

    schema = TEMPLATES[upload_type]
    table_name = schema["table"]
    transform_func = schema.get("transform")
    engine = get_engine()

    # TRUNCATE ONCE PER BATCH
    if truncate:
        with engine.connect() as conn:
            conn.execute(text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;'))
            conn.commit()

    total_processed = 0
    start_time = time.time()

    for filepath in filepaths:
        delimiter = detect_delimiter(filepath)
        
        # Inner loop uses the same robust logic as process_csv
        for chunk in pd.read_csv(
            filepath,
            chunksize=50000,
            sep=delimiter,
            on_bad_lines="warn",
            engine="c",
            low_memory=False,
            encoding_errors="replace"
        ):
            chunk.columns = chunk.columns.str.strip()
            chunk = chunk.rename(columns=schema.get("column_mapping", {}))

            if transform_func:
                chunk = transform_func(chunk)

            db_columns = schema["required"] + schema.get("optional", [])
            chunk = chunk.reindex(columns=db_columns)

            copy_chunk_to_postgres(chunk, table_name)
            total_processed += len(chunk)

            # Explicit Memory Clear
            del chunk
            gc.collect()
            
    return f"BATCH COMPLETE: {total_processed} rows"