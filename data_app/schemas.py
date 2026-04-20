from sqlalchemy import types
from transforms import transform_categ_entry, transform_stmt_entry, transform_re_consol_spec_entry

TEMPLATES = {
    "CATEG.ENTRY": {
        "table": "categ_entry",
        "required": ["stmt_id", "transaction_ref", "transaction_code", "amount", "booking_date", "category", "consol_key"],
        "optional": ["value_date", "currency", "line_no"],
        "column_mapping": {
            "Stmt Id": "stmt_id",
            "Reference": "transaction_ref",
            "Transaction Code": "transaction_code",
            "Booking Date": "booking_date",
            "Value Date": "value_date",
            "LcyAmount": "amount",
            "Product Category": "category",
            "Consol Key": "consol_key",
            "Reporting Lines": "line_no",
            "Account Currency": "currency"
        },
        # SQL Types for Postgres
        "sql_types": {
            "stmt_id": types.String(50),
            "transaction_ref": types.String(100),
            "transaction_code": types.String(20),
            "booking_date": types.Date(),
            "value_date": types.Date(),
            "amount": types.Numeric(18, 2),
            "consol_key": types.String(100),
            "category": types.String(100),
            "line_no": types.String(100),
            "currency": types.String(3)
        },
        "transform": transform_categ_entry, # Reference the function directly
        "validate": {
            "not_null": ["stmt_id", "transaction_ref"],
            "date_columns": ["booking_date", "value_date"]
        }
    },
    "STMT.ENTRY": {
        "table": "stmt_entry",
        "required": ["stmt_id", "transaction_ref", "transaction_code","amount", "booking_date", "consol_key"],
        "generated_columns": ["amount"],
        "optional": ["value_date", "category","currency", "line_no", "dr_amount", "cr_amount"],
        "column_mapping": {
            "Stmt Id": "stmt_id",
            "Account Category": "category",
            "Transaction Type": "transaction_code",
            "Booking": "booking_date",
            "Value Date": "value_date",
            "Reference": "transaction_ref",
            "DR.AMOUNT": "dr_amount",
            "CR.AMOUNT": "cr_amount",
            "Console Key": "consol_key",
            "Reporting Lines": "line_no",
        },
        "sql_types": {
            "stmt_id": types.String(50),
            "category": types.String(50),
            "transaction_code": types.String(20),
            "booking_date": types.Date(),
            "value_date": types.Date(),
            "transaction_ref": types.String(100),
            "amount": types.Numeric(18, 2), # Created during transform
            "consol_key": types.String(100),
            "line_no": types.String(50)
        },
        "transform": transform_stmt_entry,
        "validate": {
            "not_null": ["stmt_id", "transaction_ref"],
            "date_columns": ["booking_date", "value_date"]
        }
    },
    "RE.CONSOL.SPEC.ENTRY": {
        "table": "re_consol_spec_entry",
        "required": ["stmt_id", "transaction_ref", "transaction_code", "amount","asset_type", "booking_date", "consol_key"],
        "generated_columns": ["asset_type"],
        "optional": ["value_date", "currency", "line_no", "category"],
        "column_mapping": {
            "Stmt Id": "stmt_id",
            "Reference": "transaction_ref",
            "Product Category": "category",
            "Transaction Code": "transaction_code",
            "Booking Date": "booking_date",
            "Value Date": "value_date",
            "LcyAmount": "amount",
            "Consol Key": "consol_key",
            "Account Currency": "currency",
            "Reporting Lines": "line_no",
        },
        "sql_types": {
            "stmt_id": types.String(50),
            "transaction_ref": types.String(100),
            "transaction_code": types.String(20),
            "booking_date": types.Date(),
            "value_date": types.Date(),
            "amount": types.Numeric(18, 2),
            "consol_key": types.String(100),
            "category": types.String(100),
            "line_no": types.String(100),
            "asset_type": types.String(100),
            "currency": types.String(3)
        },
        "transform": transform_re_consol_spec_entry,
        "validate": {
            "not_null": ["stmt_id", "transaction_ref"],
            "date_columns": ["booking_date", "value_date"]
        }
    }
}