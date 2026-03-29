import os
import sqlite3
import json
import numpy as np
from openai import OpenAI
from db import DATABASE


def _openai_client():
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not configured on the server")
    return OpenAI(api_key=api_key)


def handle_nl_query(question, dataset):
    client = _openai_client()

    columns = dataset["columns"]
    table_name = dataset["table_name"]
    stats = dataset["stats"]
    sample = dataset["sample"]

    col_descriptions = []
    for c in columns:
        st = stats.get(c["name"], {})
        desc = f"  - {c['name']} ({c['dtype']})"
        if "min" in st:
            desc += f", range [{st['min']} – {st['max']}], mean={st['mean']}"
        if "top_values" in st:
            tops = list(st["top_values"].keys())[:3]
            desc += f", top values: {', '.join(tops)}"
        col_descriptions.append(desc)

    schema_str = "\n".join(col_descriptions)
    sample_str = json.dumps(sample[:3], indent=2, default=str)

    system_prompt = f"""You are a data analyst assistant. You help users query a SQLite database and produce insightful answers.

Table name: {table_name}
Columns:
{schema_str}

Sample rows:
{sample_str}

Your job:
1. Understand the user's natural language question.
2. Generate a valid SQLite SQL query to answer it.
3. Return ONLY a JSON object (no markdown, no explanation outside JSON) with this exact structure:
{{
  "sql": "<valid SQLite SQL query>",
  "intent": "<one sentence: what are we computing>",
  "chart_type": "<bar|line|pie|scatter|table|none>",
  "x_axis": "<column name for x axis, or null>",
  "y_axis": "<column name for y axis, or null>",
  "chart_title": "<descriptive title for chart>"
}}

Rules:
- Use only columns that exist in the table.
- Always use double quotes for column names with spaces or special chars.
- Limit results to 200 rows max using LIMIT.
- For aggregations, use GROUP BY appropriately.
- For trend/time queries, ORDER BY date column.
- For distribution questions, use COUNT(*) or SUM.
- Choose chart_type thoughtfully: bar for categories, line for time series, pie for proportions (<8 categories), scatter for correlations, table for raw data.
- If the question is unclear or unanswerable, return sql: "SELECT 'Unable to answer' as message" and chart_type: "table".
"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        temperature=0,
        max_tokens=800,
    )

    raw = response.choices[0].message.content.strip()
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    plan = json.loads(raw)
    sql = plan["sql"]

    # Execute SQL
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        data = []
        for r in rows:
            row_dict = {}
            for i, col in enumerate(cols):
                val = r[i]
                if isinstance(val, (np.integer,)):
                    val = int(val)
                elif isinstance(val, (np.floating,)) or (isinstance(val, float) and np.isnan(val)):
                    val = None if (isinstance(val, float) and np.isnan(val)) else float(val)
                row_dict[col] = val
            data.append(row_dict)
    except Exception as e:
        conn.close()
        return {
            "error": f"SQL execution failed: {str(e)}",
            "sql": sql,
            "intent": plan.get("intent", ""),
        }
    conn.close()

    # Generate summary using GPT
    summary_prompt = f"""The user asked: "{question}"
We ran this SQL: {sql}
Results ({len(data)} rows): {json.dumps(data[:20], default=str)}

Write a concise 2-3 sentence natural language summary of what the data shows. Be specific with numbers. Do not repeat the question."""

    summary_response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": summary_prompt}],
        temperature=0.3,
        max_tokens=300,
    )
    summary = summary_response.choices[0].message.content.strip()

    return {
        "question": question,
        "intent": plan.get("intent", ""),
        "sql": sql,
        "columns": cols,
        "data": data,
        "row_count": len(data),
        "summary": summary,
        "chart_type": plan.get("chart_type", "table"),
        "x_axis": plan.get("x_axis"),
        "y_axis": plan.get("y_axis"),
        "chart_title": plan.get("chart_title", "Results"),
    }