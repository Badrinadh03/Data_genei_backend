import pandas as pd
import numpy as np
import sqlite3
import json
import re
import os
import uuid
from db import DATABASE


def _json_safe(obj):
    """Ensure Flask jsonify can serialize (no numpy / odd types)."""

    def default(o):
        if isinstance(o, (np.integer,)):
            return int(o)
        if isinstance(o, (np.floating,)):
            return float(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
        return str(o)

    return json.loads(json.dumps(obj, default=default))


def sanitize_col(col):
    col = str(col).strip()
    col = re.sub(r"[^\w]", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    if not col or col[0].isdigit():
        col = "col_" + col
    return col.lower()


def infer_dtype_label(series):
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        return "float"
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    try:
        pd.to_numeric(series.dropna())
        return "numeric"
    except Exception:
        pass
    try:
        pd.to_datetime(series.dropna())
        return "datetime"
    except Exception:
        pass
    return "text"


def compute_stats(df):
    stats = {}
    for col in df.columns:
        s = df[col]
        dtype = infer_dtype_label(s)
        col_stats = {"dtype": dtype, "null_count": int(s.isna().sum()), "unique_count": int(s.nunique())}
        if dtype in ("integer", "float", "numeric"):
            num = pd.to_numeric(s, errors="coerce")
            col_stats.update({
                "min": _safe(num.min()),
                "max": _safe(num.max()),
                "mean": _safe(num.mean()),
                "median": _safe(num.median()),
                "std": _safe(num.std()),
            })
        elif dtype == "text":
            top = s.value_counts().head(5)
            col_stats["top_values"] = {str(k): int(v) for k, v in top.items()}
        stats[col] = col_stats
    return stats


def _safe(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    return val


def process_excel(filepath, original_filename):
    ext = os.path.splitext(filepath)[1].lower()

    if ext == ".csv":
        df = pd.read_csv(filepath)
    else:
        xl = pd.ExcelFile(filepath)
        sheet = xl.sheet_names[0]
        df = pd.read_excel(filepath, sheet_name=sheet)

    # Clean up
    df.columns = [sanitize_col(c) for c in df.columns]
    # Remove fully empty rows/cols
    df = df.dropna(how="all").dropna(axis=1, how="all")
    if df.empty or len(df.columns) == 0:
        raise ValueError("The file has no data rows or columns to import.")
    # Try parse dates
    for col in df.columns:
        if "date" in col or "time" in col:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass

    # Build table name (unique per upload — DB enforces UNIQUE on table_name)
    base = re.sub(r"[^\w]", "_", os.path.splitext(original_filename)[0]).lower()
    base = re.sub(r"_+", "_", base).strip("_")[:32] or "sheet"
    table_name = f"ds_{base}_{uuid.uuid4().hex[:12]}"

    # Save to SQLite
    conn = sqlite3.connect(DATABASE)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

    # Build metadata
    col_info = []
    for col in df.columns:
        col_info.append({
            "name": col,
            "dtype": infer_dtype_label(df[col]),
            "null_pct": round(df[col].isna().mean() * 100, 1),
        })

    stats = compute_stats(df)
    sample = df.head(5).replace({np.nan: None}).to_dict(orient="records")

    # Serialize dates
    def serial(obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return str(obj)

    sample_json = json.dumps(sample, default=serial)

    # Save dataset record
    conn2 = sqlite3.connect(DATABASE)
    cur = conn2.execute(
        """INSERT INTO datasets (name, table_name, row_count, col_count, columns_json, sample_json, stats_json)
           VALUES (?,?,?,?,?,?,?)""",
        (
            original_filename,
            table_name,
            len(df),
            len(df.columns),
            json.dumps(col_info),
            sample_json,
            json.dumps(stats, default=serial),
        ),
    )
    dataset_id = cur.lastrowid
    conn2.commit()
    conn2.close()

    return {
        "id": dataset_id,
        "name": original_filename,
        "table_name": table_name,
        "row_count": len(df),
        "col_count": len(df.columns),
        "columns": col_info,
        "sample": _json_safe(json.loads(sample_json)),
        "stats": _json_safe(stats),
    }