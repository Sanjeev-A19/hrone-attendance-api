"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB   (real environment variables win over a local .env)

Layout of this single file:
  1. configuration, MongoDB client, indexes
  2. time / rounding helpers (R1 - R5, R8)
  3. request models and validated types
  4. response serialisers (BSON datetime -> epoch milliseconds)
  5. employees, attendance, regularization endpoints
  6. aggregation-pipeline builders + analytics endpoints
  7. explain endpoint (re-uses the very same builders as the real endpoints)
"""
import calendar
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import AfterValidator, BaseModel, Field, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # override=False: variables already set in the environment take priority

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration, client, indexes
# --------------------------------------------------------------------------- #
# tz_aware=True makes PyMongo return aware UTC datetimes instead of naive ones.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=3000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]  # R6
NON_PRESENCE = ["ABSENT", "LEAVE"]
GRACE_SECONDS = 10 * 60  # R2
OVERTIME_MIN = 30  # R3
HALF_DAY_BELOW = Decimal("4.50")  # R5
MAX_SHIFT = timedelta(hours=24)
MAX_TREND_DAYS = 92

_indexes_ready = False
_index_lock = threading.Lock()


def ensure_indexes() -> None:
    """Create every index (idempotent: same name + same spec is a no-op)."""
    global _indexes_ready
    with _index_lock:
        if _indexes_ready:
            return
        emp, att = db.employees, db.attendance_logs
        # employees
        emp.create_index([("emp_code", ASCENDING)], unique=True, name="uq_emp_code")
        emp.create_index([("department", ASCENDING), ("emp_code", ASCENDING)], name="dept_emp")
        emp.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="dept_joined")
        emp.create_index([("joined_on", ASCENDING)], name="joined")
        # attendance_logs
        att.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uq_emp_date")
        att.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_emp")
        att.create_index(
            [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], name="status_date_emp"
        )
        att.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name="emp_punch_in")
        _indexes_ready = True


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:  # DB not up yet: /health reports 503 and retries index creation
        log.warning("index creation deferred: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_request, exc: PyMongoError):
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


# --------------------------------------------------------------------------- #
# 2. Helpers: instants, IST dates, R1 - R5, R8
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    """BSON datetime -> epoch ms. Naive datetimes are treated as UTC (that is what Mongo stores)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def trunc_seconds(dt: Optional[datetime]) -> Optional[datetime]:
    return None if dt is None else as_utc(dt).replace(microsecond=0)


def now_seconds() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def now_millis() -> datetime:
    dt = datetime.now(UTC)
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)  # BSON keeps milliseconds


def hhmm_to_minutes(value: str) -> int:
    h, m = value.split(":")
    return int(h) * 60 + int(m)


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return hhmm_to_minutes(shift_end) <= hhmm_to_minutes(shift_start)


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: IST calendar date of the punch-in; an overnight shift's early-morning punch-in
    (earlier than shift_end) belongs to the shift that started the previous day."""
    ist = punch_in.astimezone(IST)
    day = ist.date()
    if is_overnight(shift_start, shift_end):
        seconds_of_day = ist.hour * 3600 + ist.minute * 60 + ist.second
        if seconds_of_day < hhmm_to_minutes(shift_end) * 60:
            day -= timedelta(days=1)
    return day.isoformat()


def _ist_at(date_str: str, hhmm: str, plus_days: int = 0) -> datetime:
    y, mo, d = map(int, date_str.split("-"))
    h, m = map(int, hhmm.split(":"))
    return datetime(y, mo, d, h, m, tzinfo=IST) + timedelta(days=plus_days)


def shift_start_dt(date_str: str, shift_start: str) -> datetime:
    return _ist_at(date_str, shift_start)


def shift_end_dt(date_str: str, shift_start: str, shift_end: str) -> datetime:
    return _ist_at(date_str, shift_end, 1 if is_overnight(shift_start, shift_end) else 0)


def round_half_up(value: Decimal, places: int) -> Decimal:
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def compute_late_minutes(punch_in: datetime, date_str: str, shift_start: str) -> int:
    """R2: late only when strictly more than 10:00 after shift_start; then floor minutes since shift_start."""
    seconds = int((trunc_seconds(punch_in) - shift_start_dt(date_str, shift_start)).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals, half-up (Decimal, never float round())."""
    seconds = int((trunc_seconds(punch_out) - trunc_seconds(punch_in)).total_seconds())
    return float(round_half_up(Decimal(seconds) / Decimal(3600), 2))


def compute_overtime(punch_out: datetime, date_str: str, shift_start: str, shift_end: str) -> int:
    """R3: floor minutes after shift_end, only when at least 30."""
    seconds = int((trunc_seconds(punch_out) - shift_end_dt(date_str, shift_start, shift_end)).total_seconds())
    minutes = seconds // 60
    return minutes if minutes >= OVERTIME_MIN else 0


def compute_half_day(work_hours: float) -> bool:
    return Decimal(str(work_hours)) < HALF_DAY_BELOW  # R5, on the already-rounded value


def derived_fields(emp: dict, date_str: str, punch_in: Optional[datetime], punch_out: Optional[datetime]) -> dict:
    """All four derived fields for a presence record (R2-R5)."""
    if punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    out = {
        "late_minutes": compute_late_minutes(punch_in, date_str, emp["shift_start"]),
        "work_hours": None,
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        wh = compute_work_hours(punch_in, punch_out)
        out["work_hours"] = wh
        out["overtime_minutes"] = compute_overtime(punch_out, date_str, emp["shift_start"], emp["shift_end"])
        out["half_day"] = compute_half_day(wh)
    return out


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = int(month[:4]), int(month[5:7])
    last = calendar.monthrange(year, mon)[1]
    return f"{month}-01", f"{month}-{last:02d}"


def count_weekdays(first: date_cls, last: date_cls) -> int:
    """Mon-Fri days in [first, last] (R7), by arithmetic."""
    if last < first:
        return 0
    total = (last - first).days + 1
    full_weeks, extra = divmod(total, 7)
    count = full_weeks * 5
    start_wd = first.weekday()  # Monday = 0
    count += sum(1 for i in range(extra) if (start_wd + i) % 7 < 5)
    return count


# --------------------------------------------------------------------------- #
# 3. Validated types and request models
# --------------------------------------------------------------------------- #
def _check_date(value: str) -> str:
    try:
        date_cls.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("must be a real calendar date YYYY-MM-DD") from exc
    return value


def _check_month(value: str) -> str:
    if int(value[:4]) < 1:
        raise ValueError("year out of range")
    return value


DateStr = Annotated[str, Field(pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"), AfterValidator(_check_date)]
MonthStr = Annotated[str, Field(pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$"), AfterValidator(_check_month)]
HHMM = Annotated[str, Field(pattern=r"^([01][0-9]|2[0-3]):[0-5][0-9]$")]
# strict=True: floats, strings and bools are rejected; the bounds reject "seconds"-looking values.
EpochMs = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
PageQ = Annotated[int, Query(ge=1, le=10**9)]
PageSizeQ = Annotated[int, Query(ge=1, le=100)]


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: DateStr

    @model_validator(mode="after")
    def _shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


def unprocessable(message: str) -> HTTPException:
    return HTTPException(status_code=422, detail=message)


# --------------------------------------------------------------------------- #
# 4. Serialisers
# --------------------------------------------------------------------------- #
def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def _history_value(key: str, value: Any) -> Any:
    return dt_to_ms(value) if key in ("punch_in", "punch_out") else value


def history_out(entry: dict) -> dict:
    return {
        "at": dt_to_ms(entry.get("at")),
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        "changes": {
            key: {"from": _history_value(key, ch.get("from")), "to": _history_value(key, ch.get("to"))}
            for key, ch in (entry.get("changes") or {}).items()
        },
    }


def record_out(doc: dict) -> dict:
    """Stored document -> API record. Missing legacy fields default (late/overtime 0, half_day false, history [])."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


# --------------------------------------------------------------------------- #
# 5. System, employees, attendance
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        ensure_indexes()  # no-op once done; recovers if the DB came up after the app
    except PyMongoError:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_millis()
    try:
        db.employees.insert_one(doc)  # uniqueness is enforced by the unique index, not by a prior read
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: PageQ = 1, page_size: PageSizeQ = 20):
    query: dict = {"department": department} if department is not None else {}
    items = (
        db.employees.find(query, {"_id": 0})
        .sort("emp_code", ASCENDING)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    total = db.employees.count_documents(query)
    return {"items": [employee_out(d) for d in items], "total": total, "page": page, "page_size": page_size}


def find_employee(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="employee not found")
    return emp


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = find_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_seconds()
    day = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": day,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(ts, day, emp["shift_start"]),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        # The unique (emp_code, date) index decides the race: exactly one insert can win.
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = find_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_seconds()
    # Most recent record whose punch_in is at or before the punch-out (also works across midnight).
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", DESCENDING)]
    )
    if rec is None:
        raise HTTPException(status_code=404, detail="no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="already punched out")
    pin = trunc_seconds(rec["punch_in"])
    if ts <= pin:
        raise unprocessable("punched_at must be after punch_in")
    if ts - pin > MAX_SHIFT:
        raise unprocessable("punched_at is more than 24 hours after punch_in")
    fields = derived_fields(emp, rec["date"], pin, ts)
    fields["punch_out"] = ts
    # Compare-and-set on "still open": of two simultaneous punch-outs only one matches.
    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None}, {"$set": fields}, return_document=ReturnDocument.AFTER
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="already punched out")
    return record_out(updated)


def attendance_find_args(emp_code, date_from, date_to, status, page, page_size):
    """filter, sort, skip, limit of GET /attendance (shared with the explain endpoint)."""
    if date_from and date_to and date_from > date_to:
        raise unprocessable("date_from must be on or before date_to")
    query: dict = {}
    if emp_code:
        query["emp_code"] = emp_code
    if date_from or date_to:
        query["date"] = {}
        if date_from:
            query["date"]["$gte"] = date_from
        if date_to:
            query["date"]["$lte"] = date_to
    if status:
        query["status"] = status
    sort = [("date", DESCENDING), ("emp_code", ASCENDING)]
    return query, sort, (page - 1) * page_size, page_size


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[Status] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    query, sort, skip, limit = attendance_find_args(emp_code, date_from, date_to, status, page, page_size)
    docs = db.attendance_logs.find(query).sort(sort).skip(skip).limit(limit)
    total = db.attendance_logs.count_documents(query) if query else db.attendance_logs.estimated_document_count()
    return {"items": [record_out(d) for d in docs], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 5b. Regularization (manual correction with audit trail)
# --------------------------------------------------------------------------- #
def plan_regularization(emp: dict, rec: dict, body: RegularizeIn) -> tuple[dict, dict]:
    """Return (fields to $set, history 'changes') or raise 422. Pure function of the stored record + request."""
    cur_status = rec["status"]
    cur_in, cur_out = trunc_seconds(rec.get("punch_in")), trunc_seconds(rec.get("punch_out"))
    new_status = body.status or cur_status

    if new_status in NON_PRESENCE:
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable(f"{new_status} clears punch times; do not supply punch_in/punch_out")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else cur_in
        new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else cur_out
        if new_in is None:
            raise unprocessable(f"status {new_status} requires a punch_in")
        if attendance_date(new_in, emp["shift_start"], emp["shift_end"]) != rec["date"]:
            raise unprocessable("punch_in must stay on the record's attendance date")
        if new_out is not None:
            if new_out <= new_in:
                raise unprocessable("punch_out must be after punch_in")
            if new_out - new_in > MAX_SHIFT:
                raise unprocessable("punch_out must be within 24 hours of punch_in")

    if new_status == cur_status and new_in == cur_in and new_out == cur_out:
        raise unprocessable("the request changes nothing")

    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out}
    new.update(derived_fields(emp, rec["date"], new_in, new_out))
    old = {
        "status": cur_status,
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day", False)),
    }
    changes = {}
    for key, new_value in new.items():
        old_value = old[key]
        if key in ("punch_in", "punch_out"):
            old_value = trunc_seconds(old_value)
        if old_value != new_value:
            changes[key] = {"from": old[key], "to": new_value}
    return new, changes


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: DateStr, body: RegularizeIn):
    emp = find_employee(emp_code)
    for _attempt in range(5):
        rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(status_code=404, detail="no record for that date")
        fields, changes = plan_regularization(emp, rec, body)
        entry = {"at": now_millis(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
        # Optimistic concurrency: apply only if the record is exactly as we read it. Every
        # regularization appends one history entry, so the history length is a version counter.
        guard = {
            "_id": rec["_id"],
            "status": rec["status"],
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(rec.get("history") or [])]},
        }
        updated = db.attendance_logs.find_one_and_update(
            guard, {"$set": fields, "$push": {"history": entry}}, return_document=ReturnDocument.AFTER
        )
        if updated is not None:
            return record_out(updated)
    raise HTTPException(status_code=409, detail="record was modified concurrently, retry")


# --------------------------------------------------------------------------- #
# 6. Analytics: pipeline builders
# --------------------------------------------------------------------------- #
WEEKDAY_NUMBERS = [2, 3, 4, 5, 6]  # $dayOfWeek: Sunday = 1 ... Saturday = 7


def _is_weekday_expr(date_field: str = "$date") -> dict:
    day = {"$dayOfWeek": {"$dateFromString": {"dateString": date_field, "format": "%Y-%m-%d"}}}
    return {"$in": [day, WEEKDAY_NUMBERS]}


def _present_expr() -> dict:
    return {"$in": ["$status", PRESENCE]}


def _present_halves_expr(weekdays_only: bool) -> dict:
    """Presence credit in half-days: full day = 2, half day = 1, otherwise 0 (integers keep sums exact)."""
    credit = {"$cond": [{"$eq": ["$half_day", True]}, 1, 2]}
    condition = {"$and": [_present_expr(), _is_weekday_expr()]} if weekdays_only else _present_expr()
    return {"$cond": [condition, credit, 0]}


def _late_minutes_expr() -> dict:
    return {"$ifNull": ["$late_minutes", 0]}


def log_metrics_projection() -> dict:
    """Per-record numbers, read from the STORED derived fields (never recomputed from punch times)."""
    has_hours = {"$and": [_present_expr(), {"$isNumber": "$work_hours"}]}
    return {
        "_id": 0,
        "present2": _present_halves_expr(weekdays_only=True),
        "late": _late_minutes_expr(),
        "late_flag": {"$cond": [{"$gt": [_late_minutes_expr(), 0]}, 1, 0]},
        "overtime": {"$ifNull": ["$overtime_minutes", 0]},
        "leave": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]},
        "on_duty": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]},
        "wh_cents": {"$cond": [has_hours, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]},
        "wh_n": {"$cond": [has_hours, 1, 0]},
    }


def employee_monthly_pipeline(emp_code: str, month: str) -> list:
    first, last = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first, "$lte": last}}},  # (emp_code, date) index
        {"$project": log_metrics_projection()},
        {
            "$group": {
                "_id": None,
                "present2": {"$sum": "$present2"},
                "leave_days": {"$sum": "$leave"},
                "late_count": {"$sum": "$late_flag"},
                "total_late_minutes": {"$sum": "$late"},
                "total_overtime_minutes": {"$sum": "$overtime"},
            }
        },
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> list:
    """Start from employees (so people with zero logs are counted), join their month of logs."""
    first, last = month_bounds(month)
    match: dict = {"joined_on": {"$lte": last}}  # R9
    if department is not None:
        match["department"] = department
    return [
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": first, "$lte": last}}},
                    {"$project": log_metrics_projection()},
                ],
                "as": "logs",
            }
        },
        # preserveNullAndEmptyArrays keeps employees that have no logs: they still count in the headcount.
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$department",
                "emps": {"$addToSet": "$emp_code"},
                "present2": {"$sum": "$logs.present2"},
                "wh_cents": {"$sum": "$logs.wh_cents"},
                "wh_n": {"$sum": "$logs.wh_n"},
                "late_count": {"$sum": "$logs.late_flag"},
                "total_late_minutes": {"$sum": "$logs.late"},
                "leave_count": {"$sum": "$logs.leave"},
                "on_duty_count": {"$sum": "$logs.on_duty"},
            }
        },
        {"$addFields": {"headcount": {"$size": "$emps"}}},
        {"$project": {"emps": 0}},
        {"$sort": {"_id": 1}},
    ]


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    first, last = month_bounds(month)
    pipeline: list = [
        {"$match": {"date": {"$gte": first, "$lte": last}, "late_minutes": {"$gt": 0}}},  # (date, emp_code) index
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1},
            }
        },
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees are dropped here
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # $rank = standard competition ranking (1, 2, 2, 4). sortBy has ONLY the metric, otherwise
        # the emp_code tie-breaker would make every row distinct and destroy the ties.
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applied AFTER ranking
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline


def department_trend_pipeline(department: str, d_from: date_cls, d_to: date_cls) -> list:
    first, last = d_from.isoformat(), d_to.isoformat()
    n_days = (d_to - d_from).days + 1
    start = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    return [
        {"$match": {"department": department}},  # (department, joined_on) index
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": first, "$lte": last}}},
                    {
                        "$project": {
                            "_id": 0,
                            "date": 1,
                            "present2": _present_halves_expr(weekdays_only=False),
                            "late_flag": {"$cond": [{"$gt": [_late_minutes_expr(), 0]}, 1, 0]},
                        }
                    },
                ],
                "as": "logs",
            }
        },
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$logs.date",
                "present2": {"$sum": "$logs.present2"},
                "late_count": {"$sum": "$logs.late_flag"},
            }
        },
        {
            "$group": {
                "_id": None,
                "rows": {"$push": {"date": "$_id", "present2": "$present2", "late_count": "$late_count"}},
            }
        },
        # Gap filling: one entry per calendar day generated inside the database ($range + $dateAdd),
        # then the matching aggregated row (if any) is attached to it.
        {
            "$project": {
                "_id": 0,
                "days": {
                    "$map": {
                        "input": {"$range": [0, n_days]},
                        "as": "i",
                        "in": {
                            "$let": {
                                "vars": {
                                    "d": {
                                        "$dateToString": {
                                            "format": "%Y-%m-%d",
                                            "date": {"$dateAdd": {"startDate": start, "unit": "day", "amount": "$$i"}},
                                        }
                                    }
                                },
                                "in": {
                                    "date": "$$d",
                                    "row": {
                                        "$arrayElemAt": [
                                            {
                                                "$filter": {
                                                    "input": "$rows",
                                                    "as": "r",
                                                    "cond": {"$eq": ["$$r.date", "$$d"]},
                                                }
                                            },
                                            0,
                                        ]
                                    },
                                },
                            }
                        },
                    }
                },
            }
        },
        {"$unwind": "$days"},
        {
            "$replaceRoot": {
                "newRoot": {
                    "date": "$days.date",
                    "present2": {"$ifNull": ["$days.row.present2", 0]},
                    "late_count": {"$ifNull": ["$days.row.late_count", 0]},
                }
            }
        },
        # R9: headcount on that day = department employees with joined_on <= day.
        {
            "$lookup": {
                "from": "employees",
                "let": {"d": "$date"},
                "pipeline": [
                    {"$match": {"department": department, "$expr": {"$lte": ["$joined_on", "$$d"]}}},
                    {"$count": "n"},
                ],
                "as": "hc",
            }
        },
        {
            "$addFields": {
                "headcount": {"$ifNull": [{"$arrayElemAt": ["$hc.n", 0]}, 0]},
                "is_working_day": _is_weekday_expr("$date"),
            }
        },
        # rate in 1/10000 as an integer, half-up: floor((present2*10000 + H) / (2H)).  (present = present2 / 2)
        {
            "$addFields": {
                "rate_int": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {
                            "$floor": {
                                "$divide": [
                                    {"$add": [{"$multiply": ["$present2", 10000]}, "$headcount"]},
                                    {"$multiply": ["$headcount", 2]},
                                ]
                            }
                        },
                        None,
                    ]
                }
            }
        },
        {"$addFields": {"has_rate": {"$cond": [{"$isNumber": "$rate_int"}, 1, 0]}}},
        {
            "$setWindowFields": {
                "sortBy": {"date": 1},
                "output": {
                    "win_sum": {"$sum": "$rate_int", "window": {"documents": [-6, 0]}},
                    "win_n": {"$sum": "$has_rate", "window": {"documents": [-6, 0]}},
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": {"$divide": ["$present2", 2]},
                "late_count": 1,
                "attendance_rate": {"$cond": [{"$isNumber": "$rate_int"}, {"$divide": ["$rate_int", 10000]}, None]},
                # mean of the (4-decimal) rates in the window, half-up: floor((2S + n) / (2n)) / 10000
                "moving_avg_7d": {
                    "$cond": [
                        {"$gt": ["$win_n", 0]},
                        {
                            "$divide": [
                                {
                                    "$floor": {
                                        "$divide": [
                                            {"$add": [{"$multiply": ["$win_sum", 2]}, "$win_n"]},
                                            {"$multiply": ["$win_n", 2]},
                                        ]
                                    }
                                },
                                10000,
                            ]
                        },
                        None,
                    ]
                },
            }
        },
        {"$sort": {"date": 1}},
    ]


# --------------------------------------------------------------------------- #
# 6b. Analytics endpoints
# --------------------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthStr):
    emp = find_employee(emp_code)
    first, last = month_bounds(month)
    start = max(date_cls.fromisoformat(first), date_cls.fromisoformat(emp["joined_on"]))  # R7
    working_days = count_weekdays(start, date_cls.fromisoformat(last))
    rows = list(db.attendance_logs.aggregate(employee_monthly_pipeline(emp_code, month)))
    agg = rows[0] if rows else {}
    present2 = int(agg.get("present2", 0))
    present_days = Decimal(present2) / 2
    pct = round_half_up(present_days * 100 / working_days, 2) if working_days else None
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": float(round_half_up(present_days, 2)),
        "leave_days": int(agg.get("leave_days", 0)),
        "late_count": int(agg.get("late_count", 0)),
        "total_late_minutes": int(agg.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(agg.get("total_overtime_minutes", 0)),
        "attendance_pct": float(pct) if pct is not None else None,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: MonthStr, department: Optional[str] = None):
    rows = db.employees.aggregate(department_summary_pipeline(month, department))
    items = []
    for r in rows:
        wh_n = int(r["wh_n"])
        avg = round_half_up(Decimal(int(r["wh_cents"])) / wh_n / 100, 2) if wh_n else None
        items.append(
            {
                "department": r["_id"],
                "headcount": int(r["headcount"]),
                "present_days": float(round_half_up(Decimal(int(r["present2"])) / 2, 2)),
                "avg_work_hours": float(avg) if avg is not None else None,
                "late_count": int(r["late_count"]),
                "total_late_minutes": int(r["total_late_minutes"]),
                "leave_count": int(r["leave_count"]),
                "on_duty_count": int(r["on_duty_count"]),
            }
        )
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: MonthStr,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    department: Optional[str] = None,
):
    rows = db.attendance_logs.aggregate(late_leaderboard_pipeline(month, limit, department))
    items = [
        {
            "rank": int(r["rank"]),
            "emp_code": r["emp_code"],
            "name": r["name"],
            "department": r["department"],
            "total_late_minutes": int(r["total_late_minutes"]),
            "late_count": int(r["late_count"]),
        }
        for r in rows
    ]
    return {"month": month, "items": items}


def validate_trend_range(d_from: str, d_to: str) -> tuple[date_cls, date_cls]:
    start, end = date_cls.fromisoformat(d_from), date_cls.fromisoformat(d_to)
    if end < start:
        raise unprocessable("'to' must be on or after 'from'")
    if (end - start).days + 1 > MAX_TREND_DAYS:
        raise unprocessable(f"range must not exceed {MAX_TREND_DAYS} days")
    return start, end


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: Annotated[DateStr, Query(alias="from")],
    to: DateStr,
):
    start, end = validate_trend_range(from_, to)
    if db.employees.count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(status_code=404, detail="unknown department")
    rows = list(db.employees.aggregate(department_trend_pipeline(department, start, end)))
    items = [
        {
            "date": r["date"],
            "is_working_day": bool(r["is_working_day"]),
            "headcount": int(r["headcount"]),
            "present_count": float(r["present_count"]),
            "late_count": int(r["late_count"]),
            "attendance_rate": r["attendance_rate"],
            "moving_avg_7d": r["moving_avg_7d"],
        }
        for r in rows
    ]
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 7. Explain: same builders as the real endpoints, raw executionStats output
# --------------------------------------------------------------------------- #
ExplainEndpoint = Literal[
    "attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"
]


def _require(**params):
    missing = [name for name, value in params.items() if value is None]
    if missing:
        raise unprocessable("missing required query parameter(s): " + ", ".join(missing))


def _explain_command(spec: dict) -> dict:
    result = db.command({"explain": spec, "verbosity": "executionStats"})
    # BSON types (dates, Int64, ObjectId...) -> plain JSON
    return json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainEndpoint,
    emp_code: Optional[str] = None,
    month: Optional[MonthStr] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[DateStr] = None,
    date_to: Optional[DateStr] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[DateStr], Query(alias="from")] = None,
    to: Optional[DateStr] = None,
    page: PageQ = 1,
    page_size: PageSizeQ = 20,
):
    if endpoint == "attendance_list":
        query, sort, skip, lim = attendance_find_args(emp_code, date_from, date_to, status, page, page_size)
        collection = "attendance_logs"
        spec = {"find": collection, "filter": query, "sort": dict(sort), "skip": skip, "limit": lim}
    else:
        if endpoint == "employee_monthly":
            _require(emp_code=emp_code, month=month)
            collection, pipeline = "attendance_logs", employee_monthly_pipeline(emp_code, month)
        elif endpoint == "department_summary":
            _require(month=month)
            collection, pipeline = "employees", department_summary_pipeline(month, department)
        elif endpoint == "late_leaderboard":
            _require(month=month)
            collection, pipeline = "attendance_logs", late_leaderboard_pipeline(month, limit, department)
        else:  # department_trend
            _require(department=department, **{"from": from_, "to": to})
            start, end = validate_trend_range(from_, to)
            collection, pipeline = "employees", department_trend_pipeline(department, start, end)
        spec = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}
    return {"endpoint": endpoint, "collection": collection, "explain": _explain_command(spec)}
