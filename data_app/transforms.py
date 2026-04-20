import pandas as pd

def transform_categ_entry(chunk):
    if "stmt_id" in chunk.columns:
        # Use "string" instead of str to preserve actual null values
        chunk["stmt_id"] = chunk["stmt_id"].astype("string").str.strip()

    if "value_date" not in chunk.columns:
         chunk["value_date"] = None

    if "booking_date" not in chunk.columns:
         chunk["booking_date"] = None

    chunk["value_date"] = pd.to_datetime(
        chunk["value_date"],
        format="%Y%m%d",
        errors="coerce"
    )

    chunk["booking_date"] = pd.to_datetime(
        chunk["booking_date"],
        format="%Y%m%d",
        errors="coerce"
    )

    chunk["amount"] = pd.to_numeric(chunk["amount"], errors="coerce").fillna(0)

    return chunk


def transform_stmt_entry(chunk):

    #print("Columns available for math:", chunk.columns.tolist())
    
    if "stmt_id" in chunk.columns:
        chunk["stmt_id"] = chunk["stmt_id"].astype("string").str.strip()

    if "dr_amount" not in chunk.columns:
        chunk["dr_amount"] = 0

    if "cr_amount" not in chunk.columns:
        chunk["cr_amount"] = 0

    if "value_date" not in chunk.columns:
         chunk["value_date"] = None

    if "booking_date" not in chunk.columns:
         chunk["booking_date"] = None

    chunk["dr_amount"] = pd.to_numeric(chunk["dr_amount"], errors="coerce").fillna(0)
    chunk["cr_amount"] = pd.to_numeric(chunk["cr_amount"], errors="coerce").fillna(0)

    # Unified amount rule (BANKING STANDARD)
    chunk["amount"] = chunk["dr_amount"] + chunk["cr_amount"]

    chunk["value_date"] = pd.to_datetime(
        chunk["value_date"],
        format="%Y%m%d",
        errors="coerce"
    )

    chunk["booking_date"] = pd.to_datetime(
        chunk["booking_date"],
        format="%Y%m%d",
        errors="coerce"
    )

    # Drop raw columns (optional clean model)
    #chunk = chunk.drop(columns=[
     #   "dr_amount",
     #   "cr_amount"
   # ], errors="ignore")

    return chunk


def transform_re_consol_spec_entry(chunk):
    # FIX: Added the 'if in columns' check to prevent KeyErrors

   # chunk = chunk.replace(r'\\N', None, regex=True)

    if "consol_key" in chunk.columns:
        chunk["asset_type"] = (
            chunk["consol_key"]
            .astype("string")
            .str.strip()
            .str.extract(r"([^.]+$)")[0]
        )

    chunk["value_date"] = pd.to_datetime(
        chunk["value_date"],
        format="%Y%m%d",
        errors="coerce"
    )

    chunk["booking_date"] = pd.to_datetime(
        chunk["booking_date"],
        format="%Y%m%d",
        errors="coerce"
    )

    chunk["amount"] = pd.to_numeric(chunk["amount"], errors="coerce").fillna(0)

    return chunk
