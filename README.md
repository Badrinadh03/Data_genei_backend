# Excel NLP Analyzer — Backend

## Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

The server runs at http://localhost:5000

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | /api/health | Health check |
| POST | /api/upload | Upload Excel/CSV file |
| GET | /api/datasets | List all datasets |
| GET | /api/datasets/:id | Get dataset details |
| DELETE | /api/datasets/:id | Delete a dataset |
| POST | /api/query | Run NL query on dataset |

### POST /api/upload
- Body: `multipart/form-data` with `file` field
- Supports: `.xlsx`, `.xls`, `.csv`

### POST /api/query
```json
{
  "dataset_id": 1,
  "question": "What is the total sales by region?",
  "api_key": "sk-..."
}
```

## Architecture
- **Flask** — REST API server
- **SQLite** — Stores datasets and query data
- **pandas** — Excel/CSV parsing and analysis
- **OpenAI GPT-4o** — Converts NL questions to SQL + generates summaries
