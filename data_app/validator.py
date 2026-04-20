import pandas as pd

class ValidationError(Exception): pass

def validate_columns(df, required):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValidationError(f"Missing: {missing}")

def validate_nulls(df, cols):
    for c in cols:
        if c in df.columns and df[c].isnull().any():
            raise ValidationError(f"Nulls in {c}")

def validate_dates(df, cols):
    for c in cols:
        if c in df.columns:
            if pd.to_datetime(df[c], errors="coerce").isna().sum() > 0:
                raise ValidationError(f"Invalid dates in {c}")