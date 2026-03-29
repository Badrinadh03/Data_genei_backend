from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
from werkzeug.utils import secure_filename
import traceback

from db import get_db, init_db
from excel_processor import process_excel
from nl_query import handle_nl_query
from insights import generate_dataset_insights

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB

with app.app_context():
    init_db()


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/api/upload", methods=["POST"])
def upload_excel():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Empty filename"}), 400

    allowed = {".xlsx", ".xls"}
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in allowed:
        return jsonify({"error": f"Unsupported file type: {ext}"}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(filepath)

    try:
        result = process_excel(filepath, filename)
        return jsonify(result), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/datasets", methods=["GET"])
def list_datasets():
    db = get_db()
    rows = db.execute(
        "SELECT id, name, table_name, row_count, col_count, columns_json, created_at FROM datasets ORDER BY created_at DESC"
    ).fetchall()
    datasets = [dict(r) for r in rows]
    for d in datasets:
        d["columns"] = json.loads(d["columns_json"])
        del d["columns_json"]
    return jsonify(datasets)


@app.route("/api/datasets/<int:dataset_id>", methods=["GET"])
def get_dataset(dataset_id):
    db = get_db()
    row = db.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,)).fetchone()
    if not row:
        return jsonify({"error": "Dataset not found"}), 404
    ds = dict(row)
    ds["columns"] = json.loads(ds["columns_json"])
    ds["sample"] = json.loads(ds["sample_json"])
    ds["stats"] = json.loads(ds["stats_json"])
    del ds["columns_json"], ds["sample_json"], ds["stats_json"]
    return jsonify(ds)


@app.route("/api/datasets/<int:dataset_id>/preview", methods=["GET"])
def preview_rows(dataset_id):
    limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    db = get_db()
    row = db.execute("SELECT table_name FROM datasets WHERE id=?", (dataset_id,)).fetchone()
    if not row:
        return jsonify({"error": "Dataset not found"}), 404
    table_name = row["table_name"]
    cur = db.execute(f'SELECT * FROM "{table_name}" LIMIT ?', (limit,))
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    out = []
    for r in rows:
        out.append({cols[i]: r[i] for i in range(len(cols))})
    return jsonify({"columns": cols, "rows": out, "limit": limit})


@app.route("/api/insights", methods=["POST"])
def insights():
    body = request.get_json()
    if not body:
        return jsonify({"error": "No JSON body"}), 400
    dataset_id = body.get("dataset_id")
    if not dataset_id:
        return jsonify({"error": "dataset_id is required"}), 400

    db = get_db()
    row = db.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,)).fetchone()
    if not row:
        return jsonify({"error": "Dataset not found"}), 404

    ds = dict(row)
    ds["columns"] = json.loads(ds["columns_json"])
    ds["stats"] = json.loads(ds["stats_json"])
    ds["sample"] = json.loads(ds["sample_json"])

    try:
        result = generate_dataset_insights(ds)
        return jsonify(result), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 503
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/datasets/<int:dataset_id>", methods=["DELETE"])
def delete_dataset(dataset_id):
    db = get_db()
    row = db.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,)).fetchone()
    if not row:
        return jsonify({"error": "Dataset not found"}), 404
    table_name = row["table_name"]
    db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    db.execute("DELETE FROM datasets WHERE id=?", (dataset_id,))
    db.commit()
    return jsonify({"success": True})


@app.route("/api/query", methods=["POST"])
def query():
    body = request.get_json()
    if not body:
        return jsonify({"error": "No JSON body"}), 400
    dataset_id = body.get("dataset_id")
    question = body.get("question", "").strip()
    if not (os.environ.get("OPENAI_API_KEY") or "").strip():
        return jsonify({"error": "Server is not configured: set OPENAI_API_KEY in backend .env"}), 503

    if not dataset_id or not question:
        return jsonify({"error": "dataset_id and question are required"}), 400

    db = get_db()
    row = db.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,)).fetchone()
    if not row:
        return jsonify({"error": "Dataset not found"}), 404

    ds = dict(row)
    ds["columns"] = json.loads(ds["columns_json"])
    ds["stats"] = json.loads(ds["stats_json"])
    ds["sample"] = json.loads(ds["sample_json"])

    try:
        result = handle_nl_query(question, ds)
        return jsonify(result), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)