import json
import os

from openai import OpenAI


def generate_dataset_insights(dataset):
    """Returns AI-generated summary and suggested questions. Uses OPENAI_API_KEY from environment."""
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not configured on the server")

    client = OpenAI(api_key=api_key)

    columns = dataset["columns"]
    stats = dataset["stats"]
    table_name = dataset["table_name"]
    row_count = dataset.get("row_count", 0)

    col_lines = []
    for c in columns:
        st = stats.get(c["name"], {})
        line = f"  - {c['name']} ({c['dtype']})"
        if "min" in st:
            line += f" min={st['min']} max={st['max']}"
        if "top_values" in st:
            line += f" examples: {list(st['top_values'].keys())[:3]}"
        col_lines.append(line)

    schema_block = "\n".join(col_lines)

    prompt = f"""You analyze a tabular dataset (stored in SQLite table "{table_name}", ~{row_count} rows).

Schema:
{schema_block}

Respond with ONLY valid JSON (no markdown fences) in this exact shape:
{{
  "summary": "2-4 sentences describing what this dataset appears to contain and the most important fields for analysis",
  "suggestions": [
    "A specific natural-language question the user could ask about this data",
    "... 7 more distinct, useful questions — mix of aggregations, trends, filters, and distributions"
  ]
}}

Rules:
- suggestions must be answerable using SQL on the given columns only
- vary question types (totals, averages, top-N, time trends if dates exist, breakdowns by category)
- use plain English, no SQL in suggestions
"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.4,
        max_tokens=1200,
    )

    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("Could not parse AI response as JSON") from None
    suggestions = data.get("suggestions") or []
    if isinstance(suggestions, str):
        suggestions = [suggestions]
    return {
        "summary": data.get("summary", "").strip(),
        "suggestions": [s for s in suggestions if isinstance(s, str) and s.strip()][:10],
    }