# Employee Attendance & Analytics API

FastAPI + MongoDB (6.0+). All code is in `app/main.py`.

```bash
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or a local .env
uvicorn app.main:app --port 8000
```
Indexes are created at startup. `/health` returns 503 until MongoDB answers.

## Status
All endpoints in `openapi.yaml` are implemented, including the explain endpoint. Part A findings are in `REVIEW.md`, answers in `DECISIONS.md`.

## Things to know
* The analytics pipelines need MongoDB 5.0+ (`$setWindowFields`, `$dateAdd`) and use `$lookup` with `localField` + `pipeline` (5.0+).
* Trend gap filling uses `$range` + `$dateAdd` inside the pipeline (not `$densify`), so a range with no data at all still returns one row per day.
* `moving_avg_7d` averages the already-rounded (4 decimal) `attendance_rate` values, as the contract words it.
* Ranges of exactly 92 days are accepted; 93 or more give 422.
* A request to PATCH that changes none of `status`, `punch_in`, `punch_out` is 422.
