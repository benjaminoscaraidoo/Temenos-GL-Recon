from db import get_engine
import pandas as pd
from sqlalchemy import text
engine = get_engine()

def table_summary(table):
    return engine.execute(f"SELECT COUNT(*) FROM {table}").fetchall()

def get_financial_dashboard_summary():
    """
    Returns a single DataFrame with counts and totals for all three main tables.
    Matches the keys expected by the app.py helper.
    """
    query = text("""
        SELECT 'Categ Entry' as "Table", COUNT(*) as "Row_Count", SUM(COALESCE(amount, 0)) as "Total_Amount" FROM categ_entry
        UNION ALL
        SELECT 'Stmt Entry', COUNT(*), SUM(COALESCE(amount, 0)) FROM stmt_entry
        UNION ALL
        SELECT 'Spec Entry', COUNT(*), SUM(COALESCE(amount, 0)) FROM re_consol_spec_entry
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    if df.empty:
        return pd.DataFrame(columns=['Table', 'Row_Count', 'Total_Amount'])

    # Add a Grand Total row at the bottom
    grand_total_rows = df['Row_Count'].sum()
    grand_total_amt = df['Total_Amount'].sum()
    
    # Append the summary row using identical column names
    new_row = pd.DataFrame([['GRAND TOTAL', grand_total_rows, grand_total_amt]], 
                           columns=['Table', 'Row_Count', 'Total_Amount'])
    
    return pd.concat([df, new_row], ignore_index=True)

def get_total_summary():
    query = """
    SELECT 'categ_entry', SUM(amount) FROM categ_entry
    UNION ALL
    SELECT 'stmt_entry', SUM(amount) FROM stmt_entry
    UNION ALL
    SELECT 're_consol_entry', SUM(amount) FROM re_consol_entry
    """
    return engine.execute(query).fetchall()

def overall_summary():
    query = """
    SELECT 'categ_entry', COUNT(*) FROM categ_entry
    UNION ALL
    SELECT 'stmt_entry', COUNT(*) FROM stmt_entry
    UNION ALL
    SELECT 're_consol_entry', COUNT(*) FROM re_consol_entry
    """
    return engine.execute(query).fetchall()

def bal_reconciliation():
    query = """
    SELECT 'categ_entry', SUM(amount) FROM categ_entry
    UNION ALL
    SELECT 'stmt_entry', SUM(amount) FROM stmt_entry
    UNION ALL
    SELECT 're_consol_entry', SUM(amount) FROM re_consol_entry
    """
    return engine.execute(query).fetchall()

def build_filter_query(filters):
    """
    Helper to construct WHERE conditions.
    """
    conditions = []
    for col, values in filters.items():
        if values:
            # Formatting values for SQL IN clause
            formatted_vals = ", ".join([f"'{str(v).replace("'", "''")}'" for v in values])
            conditions.append(f"{col} IN ({formatted_vals})")
            
    return " AND ".join(conditions) if conditions else None

def get_summary(table, filters):
    """
    Fetches filtered summary for a specific table.
    """
    where_clause = build_filter_query(filters)
    
    query = f"""
    SELECT 
        COUNT(*) as total_rows,
        SUM(COALESCE(amount, 0)) as total_amount
    FROM {table}
    """

    if where_clause:
        query += f" WHERE {where_clause}"

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)