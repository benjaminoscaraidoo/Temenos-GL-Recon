import io
import pandas as pd
from db import get_connection

def copy_chunk_to_postgres(df, table_name):
    """
    High-speed COPY implementation with robust error handling 
    and session management for large-scale T24 data.
    """
    conn = get_connection()
    # Disable autocommit to ensure we have full control over the transaction block
    conn.autocommit = False 
    cursor = conn.cursor()

    try:
        # 1. TIMEOUT PROTECTION
        # Increase timeout to 10 mins (600s). Prevents Postgres from dropping 
        # the connection during heavy disk I/O on 150MB+ files.
        cursor.execute("SET statement_timeout = '600s';")

        # 2. DATA PREPARATION
        # Ensure we are working with a clean copy to avoid memory leakage
        df_to_load = df.copy()

        # Convert NaN/NaT/None to a uniform None type so they result in NULL
        for col in df_to_load.columns:
            df_to_load[col] = df_to_load[col].astype(object).where(df_to_load[col].notna(), None)

        # 3. BUFFER GENERATION
        buffer = io.StringIO()
        df_to_load.to_csv(
            buffer,
            index=False,
            header=False,
            na_rep='',      # Critical: NULLs must be empty strings
            quoting=0,      # Minimal quoting
            doublequote=True,
            escapechar='\\'
        )
        buffer.seek(0)

        # 4. EXECUTION
        columns = list(df_to_load.columns)
        sql = f"""
            COPY {table_name} ({','.join(columns)})
            FROM STDIN WITH (
                FORMAT CSV, 
                NULL '', 
                DELIMITER ',', 
                QUOTE '"'
            )
        """

        cursor.copy_expert(sql, buffer)
        
        # Explicit Commit: If this fails, the transaction is rolled back
        conn.commit()

    except Exception as e:
        # If any part of the COPY fails, we rollback so the DB isn't left in a "zombie" state
        if conn:
            conn.rollback()
        print(f"ERROR during COPY for {table_name}: {e}")
        raise e  # Re-raise so the Celery task knows this chunk failed

    finally:
        # Always clean up connections to prevent "Too many clients" errors
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        # Clean up local memory immediately
        del buffer