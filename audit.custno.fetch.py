import os
import pyodbc

# ===== USER INPUT =====
table_suffix = input("Enter table type (e.g. STMT, CATEG, SPEC): ").strip().upper()

# ===== CONFIG =====
BASE_INPUT_FOLDER = r"C:\Temenos\T24\KPMG_AUDIT"
BASE_OUTPUT_FOLDER = r"C:\Temenos\T24\KPMG_AUDIT\OUTPUT"

INPUT_FOLDER = os.path.join(BASE_INPUT_FOLDER, table_suffix)
OUTPUT_FOLDER = os.path.join(BASE_OUTPUT_FOLDER, table_suffix)

DB_CONFIG = {
    "server": "192.168.130.15,2105",
    "database": "t24ReportDB1",
    "user": "t24reporter",
    "password": "Q5uTC$3L82%gYM"
}

if table_suffix == 'STMT':
    TABLE_NAME = "V_FBNK_STMT_ENTRY"
elif table_suffix == 'CATEG':
    TABLE_NAME = "V_FBNK_CATEG_ENTRY"
elif table_suffix == 'SPEC':
    TABLE_NAME = "V_FBNK_RE_CONSOL_SPEC000"
else:
    raise ValueError("Invalid table type")

LOOKUP_COLUMN = "RECID"
TARGET_COLUMN = "CUSTOMER_ID"

DELIMITER = "|"
ID_INDEX = 0
CHUNK_SIZE = 5000
BATCH_SIZE = 1500  # SQL Server safe limit


# ==================
def get_db_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['user']};"
        f"PWD={DB_CONFIG['password']};"
    )
    return pyodbc.connect(conn_str)


# ==================
def fetch_batch(cursor, ids):
    """
    SQL Server safe batching (handles >2100 IDs safely)
    """
    result = {}

    ids = list(ids)

    for i in range(0, len(ids), BATCH_SIZE):
        chunk = ids[i:i + BATCH_SIZE]

        placeholders = ",".join(["?"] * len(chunk))

        query = f"""
            SELECT {LOOKUP_COLUMN}, {TARGET_COLUMN}
            FROM {TABLE_NAME}
            WHERE {LOOKUP_COLUMN} IN ({placeholders})
        """

        cursor.execute(query, chunk)

        for row in cursor.fetchall():
            result[str(row[0])] = row[1]

    return result


# ==================
def process_chunk(cursor, lines, ids, outfile):
    db_map = fetch_batch(cursor, ids)

    for parts in lines:
        record_id = parts[ID_INDEX]

        customer_id = db_map.get(record_id)

        # 🔥 TRUE NULL HANDLING (no "", no "None")
        if customer_id is None:
            customer_id = ""

        new_parts = parts[:1] + [customer_id] + parts[1:]

        outfile.write("|".join(str(v) if v is not None else "" for v in new_parts) + "\n")


# ==================
def process_file(filepath, conn):
    filename = os.path.basename(filepath)
    output_path = os.path.join(OUTPUT_FOLDER, f"NEW_{filename}")

    print(f"Processing: {filename}")

    with conn.cursor() as cursor, \
         open(filepath, "r", encoding="utf-8") as infile, \
         open(output_path, "w", encoding="utf-8") as outfile:

        # ===== HEADER =====
        header_line = infile.readline().strip()

        if header_line:
            header_parts = header_line.split(DELIMITER)
            new_header = header_parts[:1] + ["CUSTOMER_ID"] + header_parts[1:]
            outfile.write("|".join(new_header) + "\n")

        # ===== BODY =====
        buffer_lines = []
        buffer_ids = set()

        for line in infile:
            line = line.strip()
            if not line:
                continue

            parts = line.split(DELIMITER)

            if len(parts) <= ID_INDEX:
                continue

            record_id = parts[ID_INDEX]

            buffer_lines.append(parts)
            buffer_ids.add(record_id)

            if len(buffer_lines) >= CHUNK_SIZE:
                process_chunk(cursor, buffer_lines, buffer_ids, outfile)
                buffer_lines.clear()
                buffer_ids.clear()

        # remaining
        if buffer_lines:
            process_chunk(cursor, buffer_lines, buffer_ids, outfile)

    print(f"Output written to: {output_path}")


# ==================
def main():
    if not os.path.exists(INPUT_FOLDER):
        raise Exception(f"Input folder does not exist: {INPUT_FOLDER}")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    conn = get_db_connection()

    try:
        for filename in os.listdir(INPUT_FOLDER):
            filepath = os.path.join(INPUT_FOLDER, filename)

            if os.path.isfile(filepath):
                process_file(filepath, conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()