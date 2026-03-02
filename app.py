import os
import sys
from dotenv import load_dotenv
load_dotenv()  # lee .env local si existe
from functools import wraps
from io import BytesIO
from datetime import date, datetime, timedelta
from typing import Optional, Tuple

from flask import (
    Flask, request, redirect, url_for, render_template_string,
    send_file, send_from_directory, session, abort
)

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from werkzeug.security import generate_password_hash, check_password_hash

from openpyxl import load_workbook, Workbook
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas

# ==============================
# FIX STATIC + APP + DB
# ==============================
def resource_path(relative_path: str) -> str:
    if getattr(sys, "frozen", False):
        return os.path.join(sys._MEIPASS, relative_path)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, relative_path)


STATIC_DIR = resource_path("static")

app = Flask(
    __name__,
    static_folder=STATIC_DIR,
    static_url_path="/static"
)
# ✅ favicon (evita error al pedir /favicon.ico)
@app.route("/favicon.ico")
def favicon():
    return send_from_directory(os.path.join(app.static_folder, "img"), "favicon.ico")

# ==============================
# DB + SECRET (local .env / Render / exe)
# ==============================

# 1) SECRET_KEY desde env (Render o .env local)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-local-caja")

# 2) Si existe DATABASE_URL => usarla (Render / .env)
db_url = os.getenv("DATABASE_URL")

if db_url:
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    # 🔐 Si es SQLite, convertir a ruta absoluta segura
    if db_url.lower().startswith("sqlite:///"):
        rel_path = db_url.replace("sqlite:///", "", 1)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        abs_path = os.path.join(base_dir, rel_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)

        app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{abs_path}"
        DB_PATH = abs_path
    else:
        app.config["SQLALCHEMY_DATABASE_URI"] = db_url
        DB_PATH = db_url
else:
    # 3) Si NO hay DATABASE_URL:
    if getattr(sys, "frozen", False):
        def get_programdata_dir(app_name: str) -> str:
            base = os.environ.get("PROGRAMDATA", r"C:\ProgramData")
            path = os.path.join(base, app_name)
            os.makedirs(path, exist_ok=True)
            return path

        DATA_DIR = get_programdata_dir("PORA")
        INSTANCE_DIR = os.path.join(DATA_DIR, "instance")
        os.makedirs(INSTANCE_DIR, exist_ok=True)
        DB_FILE = os.path.join(INSTANCE_DIR, "caja.db")

        app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{DB_FILE}"
        DB_PATH = DB_FILE
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        instance_dir = os.path.join(base_dir, "instance")
        os.makedirs(instance_dir, exist_ok=True)
        DB_FILE = os.path.join(instance_dir, "caja.db")

        app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{DB_FILE}"
        DB_PATH = DB_FILE

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)


# ==============================
# CONFIG
# ==============================
TURNS = [("MORNING", "Mañana"), ("AFTERNOON", "Tarde")]
TURN_NAMES = dict(TURNS)

CATEGORIES = [
    "Insumos urgentes",
    "Delivery / Cadete (efectivo)",
    "Mantenimiento / Varios",
    "Imprevistos",
    "Otros (requiere nota)",
]

EMPLOYEES = ["Paula", "Pato", "Lautaro", "Sofía", "Matías"]
ADMINS = ["Bernardo", "Ximena"]

MAX_CONSUMOS = 5

# Asistencia – jornada fija y grupos
JORNADA_MIN = 9 * 60 + 10  # 09:10 => 550 min

GROUP_A = {
    "morning_in": "07:55",
    "morning_out": "12:30",
    "afternoon_in": "15:55",
    "afternoon_out": "20:30",
}
GROUP_B = {
    "morning_in": "07:55",
    "morning_out": "13:00",
    "afternoon_in": "16:55",
    "afternoon_out": "21:00",
}

# Límites mensuales (pagos)
RP_LIMIT_MIN_PER_MONTH = 120                 # 2hs por mes (minutos)
SICK_LIMIT_MIN_PER_MONTH = 2 * JORNADA_MIN   # 2 días por mes (minutos)

NOVELTY_ITEMS = [
    "",  # normal
    "Tardanza",
    "Inasistencia",
    "Razones particulares",
    "Enfermedad",
    "Curso",
    "Vacaciones",
    "Delivery",
    "Otros",
]

# ==============================
# MODELOS
# ==============================
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=True)
    role = db.Column(db.String(10), default="user")  # admin / user
    is_active = db.Column(db.Integer, default=1)

class Shift(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    day = db.Column(db.Date, nullable=False)
    turn = db.Column(db.String(10), nullable=False)  # MORNING / AFTERNOON
    responsible = db.Column(db.String(50), nullable=False)

    opening_cash = db.Column(db.Integer, nullable=False)

    sales_cash = db.Column(db.Integer, default=0)   # en tu operación: efectivo NETO (ya descontó gastos)
    sales_mp = db.Column(db.Integer, default=0)
    sales_pya = db.Column(db.Integer, default=0)
    sales_rappi = db.Column(db.Integer, default=0)
    sales_apps = db.Column(db.Integer, default=0)  # legacy

    status = db.Column(db.String(10), default="OPEN")  # OPEN / CLOSED
    closed_at = db.Column(db.DateTime)

    __table_args__ = (db.UniqueConstraint("day", "turn", name="uq_day_turn"),)

class CashExpense(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shift_id = db.Column(db.Integer, db.ForeignKey("shift.id"), nullable=False)
    category = db.Column(db.String(50), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    note = db.Column(db.String(200))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class ShiftClose(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shift_id = db.Column(db.Integer, db.ForeignKey("shift.id"), unique=True)

    withdrawn_cash = db.Column(db.Integer, nullable=False)  # regla import: retirado = sales_cash
    ending_calc = db.Column(db.Integer, nullable=False, default=0)
    ending_cash = db.Column(db.Integer, nullable=False)
    difference = db.Column(db.Integer, nullable=False)

    note = db.Column(db.String(200))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    close_ok = db.Column(db.Integer, default=1)

    edited_by = db.Column(db.String(50))
    edited_at = db.Column(db.DateTime)
    edit_reason = db.Column(db.String(200))
    edit_count = db.Column(db.Integer, default=0)

class Attendance(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    day = db.Column(db.Date, nullable=False)
    employee = db.Column(db.String(50), nullable=False)

    group_code = db.Column(db.String(1))       # A / B sugerido
    mode = db.Column(db.String(10), default="AUTO")  # AUTO / MANUAL

    morning_in = db.Column(db.String(5))
    morning_out = db.Column(db.String(5))
    afternoon_in = db.Column(db.String(5))
    afternoon_out = db.Column(db.String(5))

    novelty = db.Column(db.String(40))         # una sola columna
    novelty_minutes = db.Column(db.Integer)    # RP min; Enfermedad se guarda en min internos
    notes = db.Column(db.String(200))

    __table_args__ = (db.UniqueConstraint("day", "employee", name="uq_att_day_emp"),)

class AttendanceConsumption(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    attendance_id = db.Column(db.Integer, db.ForeignKey("attendance.id"), nullable=False)
    idx = db.Column(db.Integer, nullable=False)
    item = db.Column(db.String(120))
    amount = db.Column(db.Integer)
    __table_args__ = (db.UniqueConstraint("attendance_id", "idx", name="uq_attcons_att_idx"),)

class RotationConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    week0_map = db.Column(db.String(500))  # "Paula:A,Pato:B,..." (A/B)
    created_by = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Vacation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    employee = db.Column(db.String(50), nullable=False)
    start_day = db.Column(db.Date, nullable=False)
    end_day = db.Column(db.Date, nullable=False)
class CalendarDay(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    day = db.Column(db.Date, unique=True, nullable=False)
    holiday_type = db.Column(db.String(20))  # "", "LABORABLE", "NO_LABORABLE"

# ==============================
# MIGRACIONES / SEED
# ==============================
def _table_cols(table: str):
    # PRAGMA solo existe en SQLite
    if not is_sqlite():
        return []
    with db.engine.connect() as conn:
        return [row[1] for row in conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()]

def is_sqlite():
    uri = (app.config.get("SQLALCHEMY_DATABASE_URI") or "").lower()
    return uri.startswith("sqlite:")

def ensure_columns_shift():
    cols = _table_cols("shift")
    with db.engine.connect() as conn:
        if "sales_pya" not in cols:
            conn.exec_driver_sql("ALTER TABLE shift ADD COLUMN sales_pya INTEGER DEFAULT 0")
        if "sales_rappi" not in cols:
            conn.exec_driver_sql("ALTER TABLE shift ADD COLUMN sales_rappi INTEGER DEFAULT 0")
        if "sales_apps" not in cols:
            conn.exec_driver_sql("ALTER TABLE shift ADD COLUMN sales_apps INTEGER DEFAULT 0")

def ensure_columns_shift_close():
    cols = _table_cols("shift_close")
    with db.engine.connect() as conn:
        if "close_ok" not in cols:
            conn.exec_driver_sql("ALTER TABLE shift_close ADD COLUMN close_ok INTEGER DEFAULT 1")
        if "edited_by" not in cols:
            conn.exec_driver_sql("ALTER TABLE shift_close ADD COLUMN edited_by TEXT")
        if "edited_at" not in cols:
            conn.exec_driver_sql("ALTER TABLE shift_close ADD COLUMN edited_at DATETIME")
        if "edit_reason" not in cols:
            conn.exec_driver_sql("ALTER TABLE shift_close ADD COLUMN edit_reason TEXT")
        if "edit_count" not in cols:
            conn.exec_driver_sql("ALTER TABLE shift_close ADD COLUMN edit_count INTEGER DEFAULT 0")
        if "ending_calc" not in cols:
            conn.exec_driver_sql("ALTER TABLE shift_close ADD COLUMN ending_calc INTEGER DEFAULT 0")

def ensure_columns_attendance():
    cols = _table_cols("attendance")
    with db.engine.connect() as conn:
        if "group_code" not in cols:
            conn.exec_driver_sql("ALTER TABLE attendance ADD COLUMN group_code TEXT")
        if "mode" not in cols:
            conn.exec_driver_sql("ALTER TABLE attendance ADD COLUMN mode TEXT DEFAULT 'AUTO'")
        if "novelty" not in cols:
            conn.exec_driver_sql("ALTER TABLE attendance ADD COLUMN novelty TEXT")
        if "novelty_minutes" not in cols:
            conn.exec_driver_sql("ALTER TABLE attendance ADD COLUMN novelty_minutes INTEGER")
        if "notes" not in cols:
            conn.exec_driver_sql("ALTER TABLE attendance ADD COLUMN notes TEXT")

def seed_users():
    """
    Seed idempotente (no rompe si corre más de una vez).
    En Gunicorn con 1 worker no hay drama, pero esto lo deja robusto.
    """
    try:
        existing = {u[0] for u in User.query.with_entities(User.username).all()}
        for name in EMPLOYEES:
            if name not in existing:
                db.session.add(User(username=name, role="user"))
        for name in ADMINS:
            if name not in existing:
                db.session.add(User(username=name, role="admin"))
        db.session.commit()
    except IntegrityError:
        db.session.rollback()

def init_db():
    """
    Init DB para Render (gunicorn) y local.
    - create_all() sirve para SQLite y Postgres.
    - ensure_columns_* SOLO se ejecuta en SQLite (PRAGMA).
    - seed_users() es idempotente y tolera carreras.
    """
    with app.app_context():
        db.create_all()
        if is_sqlite():
            ensure_columns_shift()
            ensure_columns_shift_close()
            ensure_columns_attendance()
        seed_users()

# Ejecutar init al levantar (Render / gunicorn y local)
init_db()

# ==============================
# AUTH
# ==============================
def current_user():
    uid = session.get("user_id")
    return User.query.get(uid) if uid else None

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user():
            return redirect(url_for("login", next=request.path))
        return fn(*args, **kwargs)
    return wrapper

def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        u = current_user()
        if not u or u.role != "admin":
            abort(403)
        return fn(*args, **kwargs)
    return wrapper

# ==============================
# UTILS
# ==============================
def parse_day_param(d_str: str):
    if not d_str:
        dd = date.today()
        return dd, dd.isoformat()
    try:
        dd = date.fromisoformat(d_str)
        return dd, d_str
    except Exception:
        dd = date.today()
        return dd, dd.isoformat()

def parse_range_params(start_s: str, end_s: str):
    today = date.today()
    default_end = today
    default_start = today - timedelta(days=14)
    start = date.fromisoformat(start_s) if start_s else default_start
    end = date.fromisoformat(end_s) if end_s else default_end
    if start > end:
        start, end = end, start
    return start, end

def to_int(v):
    v = (v or "").strip()
    return int(v) if v != "" else 0

def safe_int(v):
    v = (v or "").strip()
    if v == "":
        return None
    try:
        return int(v)
    except:
        return None

def safe_float(v):
    v = (v or "").strip().replace(",", ".")
    if v == "":
        return None
    try:
        return float(v)
    except:
        return None

def fmt_money(n) -> str:
    if n is None:
        n = 0
    try:
        n = int(n)
    except:
        n = 0
    s = f"{n:,}".replace(",", ".")
    return f"$ {s}"

app.jinja_env.filters["money"] = fmt_money

def valid_time_str(t: str) -> bool:
    if not t:
        return True
    if len(t) != 5 or t[2] != ":":
        return False
    hh, mm = t.split(":")
    if not (hh.isdigit() and mm.isdigit()):
        return False
    h = int(hh); m = int(mm)
    return 0 <= h <= 23 and 0 <= m <= 59

def time_to_min(t: str) -> Optional[int]:
    if not t or not valid_time_str(t):
        return None
    hh, mm = map(int, t.split(":"))
    return hh * 60 + mm

def diff_minutes(t_in: str, t_out: str):
    mi = time_to_min(t_in)
    mo = time_to_min(t_out)
    if mi is None or mo is None:
        return None
    return mo - mi

def fmt_minutes(m):
    if m is None:
        return "-"
    sign = "-" if m < 0 else ""
    m = abs(int(m))
    return f"{sign}{m//60:02d}:{m%60:02d}"

def emp_key(name: str) -> str:
    return (
        name.lower()
        .replace("á","a").replace("é","e").replace("í","i")
        .replace("ó","o").replace("ú","u")
        .replace(" ", "_")
    )
import re

def _parse_amount_to_int(s: str) -> int:
    # "$40.000" / "$40,000" / "$40000" -> 40000
    s = (s or "").strip()
    s = s.replace(".", "").replace(",", "")
    digits = re.findall(r"\d+", s)
    return int("".join(digits)) if digits else 0

def extract_consumptions_from_notes(notes: str, max_items: int = 5):
    """
    Detecta patrones tipo: 'lata $790', 'adelanto $40.000'
    Separadores permitidos entre items: '/', '|', ','
    Devuelve: [(item, amount_int), ...]
    """
    if not notes:
        return []
    txt = str(notes).strip()
    if "$" not in txt:
        return []

    parts = re.split(r"[\/\|,]+", txt)
    out = []
    for p in parts:
        p = p.strip()
        if "$" not in p:
            continue

        m = re.search(r"(?P<item>.*?)(\$)\s*(?P<amt>[\d\.\,]+)", p)
        if not m:
            continue

        item = (m.group("item") or "").strip() or "Consumo"
        amt = _parse_amount_to_int(m.group("amt"))
        if amt <= 0:
            continue

        out.append((item[:120], amt))
        if len(out) >= max_items:
            break

    return out
def extract_consumptions_and_clean_notes(notes: str, max_items: int = 5):
    """
    Devuelve (consumos, notes_limpias)
    - consumos: [(item, amount_int), ...]
    - notes_limpias: texto sin los $monto (mantiene lo descriptivo)
    """
    if notes is None:
        return [], None

    txt = str(notes).strip()
    if txt == "":
        return [], None

    cons = extract_consumptions_from_notes(txt, max_items=max_items)
    if not cons:
        return [], txt

    # limpiar: remover "$ 12.345" del texto pero dejar el item
    cleaned_parts = []
    parts = re.split(r"[\/\|,]+", txt)

    for p in parts:
        p = p.strip()
        if "$" in p:
            # borra el "$123" (con puntos/comas) y espacios alrededor
            p2 = re.sub(r"\$\s*[\d\.\,]+", "", p).strip()
            if p2:
                cleaned_parts.append(p2)
        else:
            if p:
                cleaned_parts.append(p)

    notes_clean = " / ".join(cleaned_parts).strip()
    return cons, (notes_clean if notes_clean else None)

# ==============================
# CAJA UTILS
# ==============================
def expenses_total(shift_id: int) -> int:
    return sum(e.amount for e in CashExpense.query.filter_by(shift_id=shift_id).all())

def cash_bruto(s: Shift) -> int:
    # Efectivo total contado en caja al cierre (incluye caja inicial).
    return int(s.sales_cash or 0)

def cash_neto(s: Shift) -> int:
    # Efectivo neto del turno: efectivo bruto - caja inicial.
    return max(0, int(s.sales_cash or 0) - int(s.opening_cash or 0))

def calc_ingreso_neto(s: Shift) -> int:
    # Ingreso neto del turno: efectivo neto + medios electrónicos (sin sumar caja inicial).
    return (
        cash_neto(s) +
        (s.sales_mp or 0) +
        (getattr(s, "sales_pya", 0) or 0) +
        (getattr(s, "sales_rappi", 0) or 0)
    )

def calc_ingreso_bruto(s: Shift, egresos: int) -> int:
    # Ingreso bruto del turno:
    # (efectivo bruto + medios electrónicos) + egresos.
    # Se suma egresos porque, al contar la caja, esos pagos ya salieron del efectivo;
    # para reconstruir el ingreso real del turno, se re-incorporan como ingreso.
    return (
        cash_bruto(s) +
        int(s.sales_mp or 0) +
        int(getattr(s, "sales_pya", 0) or 0) +
        int(getattr(s, "sales_rappi", 0) or 0) +
        int(egresos or 0)
    )

def calc_ending_calc(cash_bruto_val: int, withdrawn: int) -> int:
    # Caja final teórica (post-retiro) = efectivo bruto - retirado
    return int(cash_bruto_val or 0) - int(withdrawn or 0)

def prev_turn_of(day_obj: date, turn_code: str):
    if turn_code == "AFTERNOON":
        return day_obj, "MORNING"
    return day_obj - timedelta(days=1), "AFTERNOON"

def get_locked_opening_cash(day_obj: date, turn_code: str):
    pday, pturn = prev_turn_of(day_obj, turn_code)
    prev_shift = Shift.query.filter_by(day=pday, turn=pturn).first()
    if not prev_shift or prev_shift.status != "CLOSED":
        return None
    close_row = ShiftClose.query.filter_by(shift_id=prev_shift.id).first()
    if not close_row:
        return None

    ec = int(close_row.ending_calc or 0)
    if ec != 0:
        return ec
    er = int(close_row.ending_cash or 0)
    if er != 0:
        return er
    return 0

def can_edit_close(u: User, close_row: ShiftClose) -> bool:
    if not u or not close_row:
        return False
    if u.role == "admin":
        return True
    return (close_row.edit_count or 0) < 1

# ==============================
# ASISTENCIA: GRUPOS / VACACIONES / CUPOS
# ==============================
def week_monday(d: date) -> date:
    return d - timedelta(days=d.weekday())

def rotation_config_get() -> Optional[RotationConfig]:
    return RotationConfig.query.order_by(RotationConfig.id.desc()).first()

def parse_week0_map(s: str) -> dict:
    m = {}
    if not s:
        return m
    parts = [p.strip() for p in s.split(",") if p.strip()]
    for p in parts:
        if ":" in p:
            emp, g = p.split(":", 1)
            emp = emp.strip()
            g = g.strip().upper()
            if emp and g in ("A","B"):
                m[emp] = g
    return m

def make_week0_map_str(m: dict) -> str:
    out = []
    for emp in EMPLOYEES:
        g = (m.get(emp) or "A").upper()
        if g not in ("A","B"):
            g = "A"
        out.append(f"{emp}:{g}")
    return ",".join(out)

def group_for_employee_on_day(emp: str, d: date) -> str:
    cfg = rotation_config_get()
    if not cfg or not cfg.week0_map:
        return "A"
    week0 = parse_week0_map(cfg.week0_map)
    base = week0.get(emp, "A")
    base = base if base in ("A","B") else "A"
    monday = week_monday(d)
    monday0 = week_monday(cfg.created_at.date())
    weeks = (monday - monday0).days // 7
    if weeks % 2 == 0:
        return base
    return "B" if base == "A" else "A"

def expected_times_for_group(g: str, d: Optional[date]=None):
    # Monday-Friday: normal groups
    if d is not None and d.weekday() == 5:
        # Saturday: reduced schedule 05:05.
        # Group A works in the morning, Group B works in the afternoon.
        if g == "A":
            return {
                "morning_in": "07:55",
                "morning_out": "13:00",
                "afternoon_in": "",
                "afternoon_out": "",
            }
        return {
            "morning_in": "",
            "morning_out": "",
            "afternoon_in": "15:55",
            "afternoon_out": "21:00",
        }

    return GROUP_A if g == "A" else GROUP_B

def is_vacation(emp: str, d: date) -> bool:
    v = Vacation.query.filter(
        Vacation.employee == emp,
        Vacation.start_day <= d,
        Vacation.end_day >= d
    ).first()
    return bool(v)

def month_range(d: date) -> Tuple[date, date]:
    start = d.replace(day=1)
    if start.month == 12:
        end = date(start.year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(start.year, start.month + 1, 1) - timedelta(days=1)
    return start, end
SATURDAY_MIN = 5 * 60 + 5  # 05:05 => 305 min

def jornada_min_for_day(d: date) -> int:
    # 0=lunes ... 5=sábado ... 6=domingo
    if d.weekday() == 5:
        return SATURDAY_MIN
    return JORNADA_MIN

def get_holiday_type(d: date) -> str:
    row = CalendarDay.query.filter_by(day=d).first()
    return (row.holiday_type or "") if row else ""

def set_holiday_type(d: date, htype: str):
    row = CalendarDay.query.filter_by(day=d).first()
    if not row:
        row = CalendarDay(day=d, holiday_type=htype or "")
        db.session.add(row)
    else:
        row.holiday_type = htype or ""

def monthly_used_minutes(emp: str, d: date, novelty_name: str) -> int:
    start, end = month_range(d)
    q = Attendance.query.filter(
        Attendance.employee == emp,
        Attendance.day >= start,
        Attendance.day <= end,
        Attendance.novelty == novelty_name
    ).all()
    return sum(int(a.novelty_minutes or 0) for a in q)
def compute_work_minutes_and_flags(a) -> dict:
    emp = a.employee
    d = a.day
    warnings = []

    g = a.group_code or group_for_employee_on_day(emp, d)
    exp = expected_times_for_group(g, d)  # OJO: tu función ya recibe (g,d)
    jornada_min = jornada_min_for_day(d)
    hday = (get_holiday_type(d) or "").upper().strip()  # "", "LABORABLE", "NO_LABORABLE"

    # Vacaciones (tabla Vacation o novedad)
    if a.novelty == "Vacaciones" or is_vacation(emp, d):
        return {
            "total_worked_min": jornada_min,
            "payable_min": jornada_min,
            "tardy_min": 0,
            "rp_unpaid_min": 0,
            "sick_unpaid_min": 0,
            "warnings": ["Vacaciones"],
            "group": g,
            "exp": exp,
        }

    # Inasistencia
    if a.novelty == "Inasistencia":
        return {
            "total_worked_min": 0,
            "payable_min": 0,
            "tardy_min": 0,
            "rp_unpaid_min": 0,
            "sick_unpaid_min": 0,
            "warnings": ["Inasistencia"],
            "group": g,
            "exp": exp,
        }

    # Delivery (pago aparte) - no computa horas
    if a.novelty == "Delivery":
        return {
            "total_worked_min": 0,
            "payable_min": 0,
            "tardy_min": 0,
            "rp_unpaid_min": 0,
            "sick_unpaid_min": 0,
            "warnings": ["Delivery (pago aparte)"],
            "group": g,
            "exp": exp,
        }

    # =========================
    # Cálculo de minutos trabajados
    # =========================

    # Mañana
    if a.morning_in or a.morning_out:
        mi = a.morning_in or exp.get("morning_in")
        mo = a.morning_out or exp.get("morning_out")
        mm = diff_minutes(mi, mo) if (mi and mo) else None
    else:
        mm = 0

    # Tarde
    if a.afternoon_in or a.afternoon_out:
        ai = a.afternoon_in or exp.get("afternoon_in")
        ao = a.afternoon_out or exp.get("afternoon_out")
        ma = diff_minutes(ai, ao) if (ai and ao) else None
    else:
        ma = 0

    total_worked = 0
    for v in (mm, ma):
        if v is None:
            continue
        if v > 0:
            total_worked += v

    # Si no cargó nada (ni mañana ni tarde), asumimos jornada completa
    if not (a.morning_in or a.morning_out or a.afternoon_in or a.afternoon_out):
        total_worked = jornada_min

    # =========================
    # Tardanza (contra horario esperado)
    # =========================
    tardy_min = 0
    exp_in = time_to_min(exp.get("morning_in") or "")
    real_in = time_to_min(a.morning_in or (exp.get("morning_in") or ""))
    if exp_in is not None and real_in is not None:
        tardy_min = max(0, real_in - exp_in)

    payable = max(0, total_worked)

    # =========================
    # Reglas de cupos / descuentos
    # =========================
    rp_unpaid = 0
    sick_unpaid = 0

    if a.novelty == "Razones particulares":
        req = int(a.novelty_minutes or 0)
        used_excluding_today = max(0, monthly_used_minutes(emp, d, "Razones particulares") - req)
        remaining = max(0, RP_LIMIT_MIN_PER_MONTH - used_excluding_today)
        unpaid = max(0, req - remaining)
        rp_unpaid = unpaid
        if unpaid > 0:
            warnings.append(f"⚠ Excede RP: {unpaid} min NO pagos")
        payable = max(0, payable - unpaid)

    if a.novelty == "Enfermedad":
        req = int(a.novelty_minutes or 0)
        used_excluding_today = max(0, monthly_used_minutes(emp, d, "Enfermedad") - req)
        remaining = max(0, SICK_LIMIT_MIN_PER_MONTH - used_excluding_today)
        unpaid = max(0, req - remaining)
        sick_unpaid = unpaid
        if unpaid > 0:
            warnings.append(f"⚠ Excede Enfermedad: {unpaid} min NO pagos")
        payable = max(0, payable - unpaid)

    if a.novelty == "Curso":
        warnings.append("Curso")

    if a.novelty == "Tardanza" and tardy_min > 0:
        warnings.append(f"Tardanza: {tardy_min} min")

    # =========================
    # Validaciones “de gestión” (avisos)
    # =========================
    # Si faltan horas y no hay novedad → warning
    if payable < jornada_min and not (a.novelty or "").strip():
        warnings.append("⚠ Faltan horas: cargá novedad")

    # Si sobran horas y no hay notas → warning
    if payable > jornada_min and not (a.notes or "").strip():
        warnings.append("⚠ Horas extra: justificá en notas")

    # Mañana
    if a.morning_in or a.morning_out:
        mi = a.morning_in or (exp.get("morning_in") or "")
        mo = a.morning_out or (exp.get("morning_out") or "")
        mm = diff_minutes(mi, mo)
    else:
        mm = 0

    # Tarde
    if a.afternoon_in or a.afternoon_out:
        ai = a.afternoon_in or (exp.get("afternoon_in") or "")
        ao = a.afternoon_out or (exp.get("afternoon_out") or "")
        ma = diff_minutes(ai, ao)
    else:
        ma = 0

    # Sumar solo minutos positivos
    total_worked = 0
    for v in (mm, ma):
        if v is None:
            continue
        if v > 0:
            total_worked += v

    # Si no cargó nada en ningún bloque, asumimos jornada completa del día
    if not (a.morning_in or a.morning_out or a.afternoon_in or a.afternoon_out):
        total_worked = jornada_min

    # Tardanza: contra el inicio esperado del bloque que corresponda
    exp_start = exp.get("morning_in") or exp.get("afternoon_in") or ""
    real_start = None
    if exp.get("morning_in"):
        real_start = a.morning_in or exp_start
    elif exp.get("afternoon_in"):
        real_start = a.afternoon_in or exp_start

    exp_in = time_to_min(exp_start)
    real_in = time_to_min(real_start or "")
    tardy_min = 0
    if exp_in is not None and real_in is not None:
        tardy_min = max(0, real_in - exp_in)

    payable = max(0, int(total_worked))

    rp_unpaid = 0
    sick_unpaid = 0

    if a.novelty == "Razones particulares":
        req = int(a.novelty_minutes or 0)
        used_excluding_today = max(0, monthly_used_minutes(emp, d, "Razones particulares") - req)
        remaining = max(0, RP_LIMIT_MIN_PER_MONTH - used_excluding_today)
        unpaid = max(0, req - remaining)
        rp_unpaid = unpaid
        if unpaid > 0:
            warnings.append(f"⚠ Excede RP: {unpaid} min NO pagos")
        payable = max(0, payable - unpaid)

    if a.novelty == "Enfermedad":
        req = int(a.novelty_minutes or 0)
        used_excluding_today = max(0, monthly_used_minutes(emp, d, "Enfermedad") - req)
        remaining = max(0, SICK_LIMIT_MIN_PER_MONTH - used_excluding_today)
        unpaid = max(0, req - remaining)
        sick_unpaid = unpaid
        if unpaid > 0:
            warnings.append(f"⚠ Excede Enfermedad: {unpaid} min NO pagos")
        payable = max(0, payable - unpaid)

    if a.novelty == "Curso":
        warnings.append("Curso")

    if a.novelty == "Tardanza" and tardy_min > 0:
        warnings.append(f"Tardanza: {tardy_min} min")

    # Reglas de feriado
    if hday == "LABORABLE":
        payable = int(payable) * 2
        warnings.append("Feriado laborable: paga x2")
    elif hday == "NO_LABORABLE":
        payable = int(jornada_min)
        warnings.append("Feriado no laborable: paga normal")

    # Reglas de control: menos horas => debe haber novedad
    if total_worked < jornada_min:
        if total_worked > 0 and not (a.novelty or "").strip():
            warnings.append("⚠ Menos horas: cargá una Novedad")

    # Más horas => debe haber nota
    if total_worked > jornada_min:
        if not (a.notes or "").strip():
            warnings.append("⚠ Horas extra: cargá Nota")

    return {
        "total_worked_min": total_worked,
        "payable_min": payable,
        "tardy_min": tardy_min,
        "rp_unpaid_min": rp_unpaid,
        "sick_unpaid_min": sick_unpaid,
        "warnings": warnings,
        "group": g,
        "exp": exp,
    }

def consumptions_summary_for_attendance(att_id: int) -> Tuple[int, str]:
    cons = (
        AttendanceConsumption.query
        .filter_by(attendance_id=att_id)
        .order_by(AttendanceConsumption.idx.asc())
        .all()
    )
    total = sum(int(c.amount or 0) for c in cons)
    items = [c.item for c in cons if c.item]
    items_txt = ", ".join(items[:4])
    if len(items) > 4:
        items_txt += "..."
    return total, items_txt

# ==============================
# BASE CSS
# ==============================
BASE_CSS = """
<style>
  body{font-family:Arial, sans-serif; margin:24px auto; padding:0 12px; color:#111;}
  a{color:#5a2ca0; text-decoration:none;}
  a:hover{text-decoration:underline;}
  table{border-collapse:collapse; width:100%;}
  th,td{border:1px solid #ddd; padding:10px; font-size:14px; vertical-align:middle;}
  th{background:#f3f3f3; text-align:left;}
  .muted{color:#666; font-size:12px;}
  .btn{display:inline-block; padding:8px 12px; border:1px solid #bbb; border-radius:10px; background:#f6f6f6; cursor:pointer; color:#111; text-decoration:none;}
  .btn:hover{background:#ececec;}
  .badge{padding:3px 10px; border-radius:999px; font-size:12px; display:inline-block; border:1px solid transparent;}
  .badge-ok{background:#c8f3d6; color:#0f5132; border-color:#85d9a1;}
  .badge-warn{background:#ffe2b8; color:#7a4b00; border-color:#ffbf66;}
  .badge-open{background:#ffe9a6; color:#5f4b00; border-color:#ffd255;}
  .badge-closed{background:#bfead3; color:#0f5132; border-color:#7ad2a4;}
  .box{border:1px solid #d0d0d0; border-radius:12px; padding:12px; background:#fafafa;}
  .row{display:flex; gap:14px; flex-wrap:wrap; align-items:center;}
  .top{display:flex; gap:16px; align-items:center; justify-content:space-between; flex-wrap:wrap;}
  .logo{display:flex; gap:12px; align-items:center;}
  .logo img{height:60px; border-radius:10px;}
  h1,h2,h3{margin:12px 0;}
  input,select,button,textarea{font-family:inherit;}
  .net-soft{background:#dff5e6;}
  .vac-row{background:#d9d9d9;}
  .warn-row{background:#ffd9c7;}
  .preset{color:#777 !important;}
  .preset::-webkit-datetime-edit{color:#777;}
  .preset:hover{color:#111 !important;}
  .preset:hover::-webkit-datetime-edit{color:#111;}
  .chip{display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; border:1px solid #bbb;}
  .chip-rp{background:#ffd7a6; border-color:#ffb866;}
  .chip-vac{background:#cfcfcf; border-color:#a9a9a9;}
  .chip-curso{background:#cfe7ff; border-color:#89bfff;}
</style>
"""

# ==============================
# AUTH TEMPLATES
# ==============================
LOGIN_HTML = """
<!doctype html>
<html lang="es">
<head><meta charset="utf-8"><title>Login</title>
""" + BASE_CSS + """
<style>
  body{max-width:420px;}
  input,select,button{padding:10px;font-size:14px;width:100%;margin:6px 0}
  .card{border:1px solid #ddd;border-radius:12px;padding:16px;background:#fafafa}
  .err{color:#842029}
  .holiday-bg{background:#ffe3ec;}
  .holiday-bg table tr{background:#fff;}
</style>
</head>
<body>
<div class="card">
  <h2 style="margin-top:0">Ingresar</h2>
  {% if err %}<p class="err"><b>{{err}}</b></p>{% endif %}
  <form method="post">
    <label>Usuario</label>
    <select name="username" required>
      {% for u in users %}
        <option value="{{u}}">{{u}}</option>
      {% endfor %}
    </select>
    <label>Contraseña</label>
    <input type="password" name="password" required>
    <button class="btn">Entrar</button>
  </form>
  <p class="muted">Primera vez: configurá tu contraseña.</p>
  <p><a href="{{ url_for('setup_password') }}">Configurar contraseña</a></p>
</div>
</body>
</html>
"""

SETUP_HTML = """
<!doctype html>
<html lang="es">
<head><meta charset="utf-8"><title>Configurar contraseña</title>
""" + BASE_CSS + """
<style>
  body{max-width:480px;}
  input,select,button{padding:10px;font-size:14px;width:100%;margin:6px 0}
  .card{border:1px solid #ddd;border-radius:12px;padding:16px;background:#fafafa}
  .err{color:#842029}
  .ok{color:#0f5132}
  .holiday-bg{background:#ffe3ec;}
  .holiday-bg table tr{background:#fff;}
</style>
</head>
<body>
<div class="card">
  <h2 style="margin-top:0">Configurar contraseña</h2>
  {% if err %}<p class="err"><b>{{err}}</b></p>{% endif %}
  {% if msg %}<p class="ok"><b>{{msg}}</b></p>{% endif %}
  <form method="post">
    <label>Usuario</label>
    <select name="username" required>
      {% for u in users %}
        <option value="{{u}}">{{u}}</option>
      {% endfor %}
    </select>
    <label>Nueva contraseña</label>
    <input type="password" name="p1" required>
    <label>Confirmar contraseña</label>
    <input type="password" name="p2" required>
    <button class="btn">Guardar</button>
  </form>
  <p><a href="{{ url_for('login') }}">Volver a login</a></p>
</div>
</body>
</html>
"""

@app.route("/health")
def health():
    return "ok"

@app.route("/login", methods=["GET","POST"])
def login():
    users = [u.username for u in User.query.filter_by(is_active=1).order_by(User.username.asc()).all()]
    err = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        u = User.query.filter_by(username=username, is_active=1).first()
        if not u or not u.password_hash:
            err = "Usuario no configurado. Configurá tu contraseña primero."
        elif not check_password_hash(u.password_hash, password):
            err = "Contraseña incorrecta."
        else:
            session["user_id"] = u.id
            nxt = request.args.get("next") or url_for("home")
            return redirect(nxt)
    return render_template_string(LOGIN_HTML, users=users, err=err)

@app.route("/setup", methods=["GET","POST"])
def setup_password():
    users = [u.username for u in User.query.filter_by(is_active=1).order_by(User.username.asc()).all()]
    err = None
    msg = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        p1 = request.form.get("p1") or ""
        p2 = request.form.get("p2") or ""
        if p1 != p2:
            err = "Las contraseñas no coinciden."
        elif len(p1) < 4:
            err = "Contraseña demasiado corta (mínimo 4)."
        else:
            u = User.query.filter_by(username=username, is_active=1).first()
            if not u:
                err = "Usuario inválido."
            else:
                u.password_hash = generate_password_hash(p1)
                db.session.commit()
                msg = "Contraseña guardada. Ya podés ingresar."
    return render_template_string(SETUP_HTML, users=users, err=err, msg=msg)

@app.route("/logout")
def logout():
    session.pop("user_id", None)
    return redirect(url_for("login"))

# ==============================
# PANEL
# ==============================
HOME_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>PORA - Panel</title>
  """ + BASE_CSS + """
  <style>
    body{max-width:980px;}
    .brand{display:flex; gap:14px; align-items:center; flex-wrap:wrap;}
    .brand img{height:70px; border-radius:10px;}
    .cards{display:grid; grid-template-columns:repeat(3, minmax(220px, 1fr)); gap:14px; margin-top:18px;}
    @media (max-width: 820px){ .cards{grid-template-columns:1fr;} }
    a.card{border:1px solid #ddd; border-radius:14px; padding:16px; color:#111; background:#fafafa; display:block;}
    a.card:hover{background:#f0f0f0;}
    .t{font-size:18px; font-weight:bold; margin:0 0 6px;}
    .d{color:#555; margin:0; font-size:13px; line-height:1.35;}
    .holiday-bg{background:#ffe3ec;}
  .holiday-bg table tr{background:#fff;}
</style>
</head>
<body>
  <div class="top">
    <div class="brand">
      <img src="{{ url_for('static', filename='img/pora_logo.png') }}" alt="PORA">
      <div>
        <h1 style="margin:0;">PORA - Panel</h1>
        <div class="muted">Usuario: <b>{{username}}</b> ({{role}})</div>
      </div>
    </div>
    <div><a class="btn" href="{{ url_for('logout') }}">Salir</a></div>
  </div>

  <div class="cards">
    <a class="card" href="{{ url_for('caja_index') }}">
      <p class="t">Control de Caja</p>
      <p class="d">Turnos, cierres, edición, export e import.</p>
    </a>

    <a class="card" href="{{ url_for('asistencia') }}">
      <p class="t">Asistencia</p>
      <p class="d">Carga diaria + resumen + export + import.</p>
    </a>

    <a class="card" href="{{ url_for('stock') }}">
      <p class="t">Control de Stock</p>
      <p class="d">En construcción.</p>
    </a>
  </div>
</body>
</html>
"""

@app.route("/")
@login_required
def home():
    u = current_user()
    return render_template_string(HOME_HTML, username=u.username, role=u.role)

@app.route("/stock")
@login_required
def stock():
    return "<h2>Stock (en construcción)</h2><p><a href='/'>Volver</a></p>"

# ============================================================
# =====================  CAJA (UI)  ===========================
# ============================================================

def get_caja_summary(limit=200, start: Optional[date]=None, end: Optional[date]=None,
                     turn: str="ALL", responsible: str="ALL"):
    rows = []
    q = (
        db.session.query(Shift, ShiftClose)
        .join(ShiftClose, ShiftClose.shift_id == Shift.id)
        .filter(Shift.status == "CLOSED")
    )
    if start:
        q = q.filter(Shift.day >= start)
    if end:
        q = q.filter(Shift.day <= end)
    if turn and turn != "ALL":
        q = q.filter(Shift.turn == turn)
    if responsible and responsible != "ALL":
        q = q.filter(Shift.responsible == responsible)

    q = q.order_by(Shift.day.desc(), Shift.turn.asc()).limit(limit).all()

    for s, c in q:
        exp = expenses_total(s.id)
        neto = calc_ingreso_neto(s)
        bruto = calc_ingreso_bruto(s, exp)

        rows.append({
            "day": s.day.isoformat(),
            "turn_code": s.turn,
            "turn_name": TURN_NAMES.get(s.turn, s.turn),
            "responsible": s.responsible,
            "opening_cash": int(s.opening_cash or 0),
            "sales_cash": int(s.sales_cash or 0),
            "sales_mp": int(s.sales_mp or 0),
            "sales_pya": int(getattr(s, "sales_pya", 0) or 0),
            "sales_rappi": int(getattr(s, "sales_rappi", 0) or 0),
            "ventas_bruto": int(bruto),
            "ventas_neto": int(neto),
            "expenses": int(exp),
            "withdrawn": int(c.withdrawn_cash or 0),
            "ending_real": int(c.ending_cash or 0),
            "difference": int(c.difference or 0),
            "close_ok": int(c.close_ok or 1),
        })
    return rows

CAJA_INDEX_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Control de Caja - PORA</title>
  {{ base_css|safe }}
  <style>
    body{max-width:1300px;}
    .filters{display:flex; gap:10px; flex-wrap:wrap; align-items:end; margin:8px 0 14px;}
    .filters label{display:block; font-size:12px; color:#666;}
    .smallnote{font-size:12px; color:#666; margin-top:6px;}
    .holiday-bg{background:#ffe3ec;}
  .holiday-bg table tr{background:#fff;}
</style>
</head>
<body class="{{ 'holiday-bg' if hday else '' }}">

<div class="top">
  <div class="logo">
    <img src="{{ url_for('static', filename='img/pora_logo.png') }}" alt="PORA">
    <div>
      <h1 style="margin:0;">Control de Caja</h1>
      <div class="muted"><a href="{{ url_for('home') }}">← Volver al menú</a></div>

      <form method="get" action="{{ url_for('caja_index') }}" style="margin-top:6px; display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
        <label class="muted">Fecha:</label>
        <input type="date" name="d" value="{{d}}">
        <button class="btn" type="submit">Ir</button>
      </form>
    </div>
  </div>
  <div class="row">
    <a class="btn" href="{{ url_for('export_excel') }}">Exportar Excel</a>
    <a class="btn" href="{{ url_for('export_pdf') }}">Exportar PDF</a>
    <a class="btn" href="{{ url_for('import_caja') }}">Importar Excel</a>
  </div>
</div>

<h2>Turnos del día</h2>
<table>
  <tr>
    <th>Turno</th>
    <th>Responsable</th>
    <th>Estado</th>
    <th>Cierre</th>
    <th>Ventas netas</th>
    <th>Efectivo disponible</th>
    <th>Acción</th>
  </tr>

  {% for code,name in turns %}
    {% set s = shifts.get(code) %}
    <tr>
      <td><b>{{name}}</b></td>
      <td>{{ s.responsible if s else "-" }}</td>
      <td>
        {% if not s %}
          <span class="badge badge-open">No abierto</span>
        {% elif s.status == "OPEN" %}
          <span class="badge badge-open">Abierto</span>
        {% else %}
          <span class="badge badge-closed">Cerrado</span>
        {% endif %}
      </td>
      <td>
        {% if s and s.status == "CLOSED" %}
          {% set c = closes.get(s.id) %}
          {% if c and c.close_ok == 1 %}
            <span class="badge badge-ok">OK</span>
          {% else %}
            <span class="badge badge-warn">NO OK</span>
          {% endif %}
        {% else %}
          -
        {% endif %}
      </td>

      <td>
        {% if s and s.status == "CLOSED" %}
          {{ ventas_netas_map.get(s.id, 0)|money }}
        {% else %}-{% endif %}
      </td>

      <td>
        {% if s and s.status == "CLOSED" %}
          {{ efectivo_disp_map.get(s.id, 0)|money }}
        {% else %}-{% endif %}
      </td>

      <td>
        {% if not s %}
          <a href="{{ url_for('open_shift', turn=code, d=d) }}">Abrir</a>
        {% else %}
          {% if s.status == "OPEN" %}
            <a href="{{ url_for('shift', id=s.id, d=d) }}">Entrar</a>
          {% else %}
            <a href="{{ url_for('shift', id=s.id, d=d) }}">Ver</a>
            {% if can_edit_map.get(s.id) %}
              | <a href="{{ url_for('edit_all', id=s.id, d=d) }}">Editar</a>
            {% endif %}
          {% endif %}
        {% endif %}
      </td>
    </tr>
  {% endfor %}
</table>

{% if efectivo_total_dia is not none %}
  <div class="smallnote">
    <b>Total efectivo disponible del día:</b> {{ efectivo_total_dia|money }}
    <br><b>Total ingreso del día (neto):</b> {{ ingreso_neto_total_dia|money }}
  </div>
{% endif %}

<h2 style="margin-top:18px;">Resumen de cierres</h2>

<form method="get" action="{{ url_for('caja_index') }}" class="filters">
  <input type="hidden" name="d" value="{{d}}">

  <div>
    <label>Desde</label>
    <input type="date" name="start" value="{{start}}">
  </div>

  <div>
    <label>Hasta</label>
    <input type="date" name="end" value="{{end}}">
  </div>

  <div>
    <label>Turno</label>
    <select name="turn">
      <option value="ALL" {% if turn=='ALL' %}selected{% endif %}>Todos</option>
      <option value="MORNING" {% if turn=='MORNING' %}selected{% endif %}>Mañana</option>
      <option value="AFTERNOON" {% if turn=='AFTERNOON' %}selected{% endif %}>Tarde</option>
    </select>
  </div>

  <div>
    <label>Responsable</label>
    <select name="resp">
      <option value="ALL" {% if resp=='ALL' %}selected{% endif %}>Todos</option>
      {% for r in responsibles %}
        <option value="{{r}}" {% if resp==r %}selected{% endif %}>{{r}}</option>
      {% endfor %}
    </select>
  </div>

  <div>
    <button class="btn" type="submit">Aplicar</button>
  </div>
</form>

<table>
  <tr>
    <th>Fecha</th>
    <th>Turno</th>
    <th>Responsable</th>
    <th>Caja Inicial</th>
    <th>Efectivo bruto</th>
    <th>Ventas Mercado Pago</th>
    <th>Ventas Pedidos Ya</th>
    <th>Ventas Rappi</th>
    <th>Egresos</th>
    <th>Ventas totales</th>
    <th class="net-soft">Ventas netas</th>
    <th>Retirado</th>
    <th>Caja Final (Real)</th>
  </tr>

  {% for row in summary %}
    <tr>
      <td>{{ row.day }}</td>
      <td>{{ row.turn_name }}</td>
      <td>{{ row.responsible }}</td>
      <td>{{ row.opening_cash|money }}</td>
      <td>{{ row.sales_cash|money }}</td>
      <td>{{ row.sales_mp|money }}</td>
      <td>{{ row.sales_pya|money }}</td>
      <td>{{ row.sales_rappi|money }}</td>
      <td>{{ row.expenses|money }}</td>
      <td>{{ row.ventas_bruto|money }}</td>
      <td class="net-soft"><b>{{ row.ventas_neto|money }}</b></td>
      <td>{{ row.withdrawn|money }}</td>
      <td>{{ row.ending_real|money }}</td>
    </tr>
  {% endfor %}

  {% if not summary %}
    <tr><td colspan="13" class="muted">Sin cierres para el filtro seleccionado.</td></tr>
  {% endif %}
</table>

<p class="muted">DB: <code>{{db_path}}</code></p>
</body>
</html>
"""

OPEN_HTML = """
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Abrir turno</title>
{{ base_css|safe }}
<style>
  body{max-width:540px;}
  input,button{padding:10px; font-size:14px;}
  .holiday-bg{background:#ffe3ec;}
  .holiday-bg table tr{background:#fff;}
</style>
</head>
<body>
<a href="{{ url_for('caja_index', d=d) }}">← Volver</a>
<h3>Abrir turno {{turn_name}} ({{day}})</h3>

<div class="box">
  <form method="post">
    <input type="hidden" name="d" value="{{d}}">

    {% if locked_opening is not none %}
      <p><b>Caja inicial</b> (del cierre anterior):</p>
      <p><input type="number" name="opening_cash" value="{{locked_opening}}" readonly></p>
      <p class="muted">No se puede editar.</p>
    {% else %}
      <p><b>Caja inicial</b> (no hay cierre previo):</p>
      <p><input type="number" name="opening_cash" min="0" required></p>
      <p class="muted">Solo se usa si es el primer turno sin historial.</p>
    {% endif %}

    <p><b>Responsable:</b></p>
    <input name="responsible" required style="width:100%;" value="{{default_responsible}}" readonly>

    <br><br>
    <button class="btn">Abrir</button>
  </form>
</div>
</body>
</html>
"""

SHIFT_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Turno</title>
  {{ base_css|safe }}
  <style>
    body{max-width:1100px;}
    input,select,button,textarea{padding:8px; font-size:14px;}
    .bad{color:#842029; font-weight:bold;}
    .good{color:#0f5132; font-weight:bold;}
    .holiday-bg{background:#ffe3ec;}
  .holiday-bg table tr{background:#fff;}
</style>
</head>
<body>
<a href="{{ url_for('caja_index', d=d) }}">← Volver</a>
<h2>Turno {{turn_name}} — {{s.day}}</h2>

<p>
  Responsable: <b>{{s.responsible}}</b> |
  Estado: <b>{{s.status}}</b> |
  Caja inicial: <b>{{ s.opening_cash|money }}</b>
</p>

{% if close and can_edit %}
  <p class="muted">
    <a class="btn" href="{{ url_for('edit_all', id=s.id, d=d) }}">Editar (todo)</a>
    {% if close.edit_count %}
      <span class="muted">Ediciones: {{close.edit_count}}</span>
    {% endif %}
  </p>
{% elif close and not can_edit %}
  <p class="muted">Este turno ya no puede editarse con tu usuario.</p>
{% endif %}

<h3>Ventas</h3>
<form method="post" action="{{ url_for('sales', id=s.id, d=d) }}">
  Efectivo total (bruto):
  <input type="number" name="sales_cash" min="0" value="{{s.sales_cash}}" {{'disabled' if s.status=='CLOSED' else ''}}><br><br>
  <div class="muted">Efectivo neto (auto): <b>{{ ((s.sales_cash - s.opening_cash) if (s.sales_cash - s.opening_cash) > 0 else 0)|money }}</b></div><br>

  Ventas Mercado Pago:
  <input type="number" name="sales_mp" min="0" value="{{s.sales_mp}}" {{'disabled' if s.status=='CLOSED' else ''}}><br><br>

  Ventas Pedidos Ya:
  <input type="number" name="sales_pya" min="0" value="{{s.sales_pya}}" {{'disabled' if s.status=='CLOSED' else ''}}><br><br>

  Ventas Rappi:
  <input type="number" name="sales_rappi" min="0" value="{{s.sales_rappi}}" {{'disabled' if s.status=='CLOSED' else ''}}><br><br>

  {% if s.status != 'CLOSED' %}
    <button class="btn">Guardar ventas</button>
  {% endif %}
</form>

<h3>Egresos (caja chica, efectivo)</h3>
{% if s.status != 'CLOSED' %}
<form method="post" action="{{ url_for('expense', id=s.id, d=d) }}">
  <select name="category">
    {% for c in categories %}<option>{{c}}</option>{% endfor %}
  </select>
  <input type="number" name="amount" min="1" required>
  <input name="note" placeholder="nota (obligatoria si Otros)">
  <button class="btn">Agregar</button>
</form>
{% else %}
<p class="muted">Turno cerrado: no se pueden agregar egresos desde acá. Usá Editar (todo).</p>
{% endif %}

<table style="margin-top:10px;">
  <tr><th>Categoría</th><th>Monto</th><th>Nota</th></tr>
  {% for e in expenses %}
    <tr><td>{{e.category}}</td><td>{{e.amount|money}}</td><td>{{e.note or ''}}</td></tr>
  {% endfor %}
  {% if not expenses %}
    <tr><td colspan="3" class="muted">Sin egresos</td></tr>
  {% endif %}
</table>

<h3>Cierre</h3>

<div class="box">
  <div class="row">
    <div><b>Egresos total:</b> {{ egresos_total|money }}</div>
    <div><b>Ingreso total (bruto):</b> {{ ingreso_bruto|money }}</div>
    <div><b>Ingreso neto:</b> {{ ingreso_neto|money }}</div>
    <div><b>Efectivo disponible:</b> {{ efectivo_disponible|money }}</div>
  </div>
  <div class="muted" style="margin-top:6px;">
    Ingreso neto = (Efectivo neto + MP + PedidosYa + Rappi). Efectivo neto = Efectivo bruto - Caja inicial.<br>
    Ingreso total (bruto) = (Efectivo bruto + MP + PedidosYa + Rappi) + Egresos.<br>
    Efectivo disponible = Efectivo bruto (lo contado en caja).
  </div>
</div>

{% if close %}
  <div class="box" style="margin-top:10px;">
    <div class="row">
      <div><b>Retirado:</b> {{close.withdrawn_cash|money}}</div>
      <div><b>Caja final REAL:</b> {{close.ending_cash|money}}</div>
      <div><b>Diferencia:</b> <span class="{{ 'good' if close.difference==0 else 'bad' }}">{{ close.difference|money }}</span></div>
      <div><b>Estado:</b> {{ 'OK' if close.close_ok==1 else 'NO OK' }}</div>
    </div>
    <div class="muted" style="margin-top:6px;"><b>Obs:</b> {{close.note or ''}}</div>

    {% if close.edited_by %}
      <div class="muted" style="margin-top:6px;">
        <b>Editado:</b> {{close.edited_by}} — {{close.edited_at}} — {{close.edit_reason}}
      </div>
    {% endif %}
  </div>
{% else %}

  <form method="post" action="{{ url_for('close_shift', id=s.id, d=d) }}" style="margin-top:12px;" id="closeForm">
    <div class="box">
      <div class="row">
        <div>
          <label><b>Retirado (dueño/gerencia)</b></label><br>
          <input type="number" name="withdrawn" min="0" required id="withdrawn">
        </div>

        <div>
          <label><b>Estado</b></label><br>
          <select name="ok_status" id="ok_status" required>
            <option value="" selected>Seleccionar...</option>
            <option value="OK">OK</option>
            <option value="NOOK">NO OK</option>
          </select>
        </div>

        <div id="real_box" style="display:none;">
          <label><b>Caja final REAL</b></label><br>
          <input type="number" name="ending_real" min="0" id="ending_real">
          <div class="muted">Obligatoria si NO OK</div>
        </div>
      </div>

      <div style="margin-top:10px;">
        <label><b>Observación (si NO OK)</b></label><br>
        <input name="note" id="note" style="width:100%;" disabled placeholder="Explicar el motivo de la diferencia...">
      </div>
    </div>

    <br>
    <button class="btn">Cerrar turno</button>
  </form>

  <script>
    const okStatusEl = document.getElementById("ok_status");
    const realBox = document.getElementById("real_box");
    const endingRealEl = document.getElementById("ending_real");
    const noteEl = document.getElementById("note");
    const form = document.getElementById("closeForm");

    function updateUI() {
      const status = okStatusEl.value;
      if (status === "NOOK") {
        realBox.style.display = "block";
        endingRealEl.required = true;
        noteEl.disabled = false;
        noteEl.required = true;
      } else if (status === "OK") {
        realBox.style.display = "none";
        endingRealEl.required = false;
        endingRealEl.value = "";
        noteEl.disabled = true;
        noteEl.required = false;
        noteEl.value = "";
      } else {
        realBox.style.display = "none";
        endingRealEl.required = false;
        noteEl.disabled = true;
        noteEl.required = false;
      }
    }

    okStatusEl.addEventListener("change", updateUI);

    form.addEventListener("submit", (e) => {
      const status = okStatusEl.value;
      if (!status) {
        alert("Seleccioná OK o NO OK.");
        e.preventDefault();
        return;
      }
      if (status === "NOOK") {
        if (!endingRealEl.value) {
          alert("Si es NO OK, cargá la caja final REAL.");
          e.preventDefault();
          return;
        }
        if (!noteEl.value.trim()) {
          alert("Si es NO OK, completá la observación.");
          e.preventDefault();
          return;
        }
      }
    });

    updateUI();
  </script>

{% endif %}

</body>
</html>
"""

EDIT_ALL_HTML = """
<!doctype html>
<html lang="es">
<head><meta charset="utf-8"><title>Editar turno</title>
{{ base_css|safe }}
<style>
  body{max-width:1200px;}
  input,select,button,textarea{padding:8px; font-size:14px;}
  .row{display:grid; grid-template-columns:repeat(4, 1fr); gap:10px;}
  .row2{display:grid; grid-template-columns:1fr 2fr; gap:10px;}
  .right{text-align:right;}
  .holiday-bg{background:#ffe3ec;}
  .holiday-bg table tr{background:#fff;}
</style>
</head>
<body>

<a href="{{ url_for('shift', id=s.id, d=d) }}">← Volver</a>
<h2 style="margin-bottom:6px;">Editar (todo) — {{turn_name}} {{s.day}}</h2>
<p class="muted">Usuario: <b>{{user.username}}</b> ({{user.role}}) — Ediciones previas: {{close.edit_count}}</p>

<form method="post">
  <div class="box">
    <h3>Datos del turno</h3>
    <div class="row">
      <div>
        <label>Responsable</label><br>
        <input type="text" name="responsible" value="{{s.responsible}}" required style="width:100%;">
      </div>
      <div>
        <label>Caja inicial</label><br>
        <input type="number" name="opening_cash" min="0" value="{{s.opening_cash}}" required style="width:100%;">
      </div>
      <div>
        <label>Turno</label><br>
        <input type="text" value="{{turn_name}}" readonly style="width:100%;">
      </div>
      <div>
        <label>Fecha</label><br>
        <input type="text" value="{{s.day}}" readonly style="width:100%;">
      </div>
    </div>
    <p class="muted">Cambiar CI impacta cálculos del turno siguiente.</p>
  </div>

  <div class="box">
    <h3>Ventas</h3>
    <div class="row">
      <div>Ventas efectivo (neto)<br><input type="number" name="sales_cash" min="0" value="{{s.sales_cash}}" style="width:100%;"></div>
      <div>Ventas Mercado Pago<br><input type="number" name="sales_mp" min="0" value="{{s.sales_mp}}" style="width:100%;"></div>
      <div>Ventas Pedidos Ya<br><input type="number" name="sales_pya" min="0" value="{{s.sales_pya}}" style="width:100%;"></div>
      <div>Ventas Rappi<br><input type="number" name="sales_rappi" min="0" value="{{s.sales_rappi}}" style="width:100%;"></div>
    </div>
  </div>

  <div class="box">
    <h3>Egresos (editar / borrar / agregar)</h3>
    <table>
      <tr><th>Eliminar</th><th>Categoría</th><th class="right">Monto</th><th>Nota</th></tr>
      {% for e in expenses %}
        <tr>
          <td><input type="checkbox" name="del_{{e.id}}"></td>
          <td>
            <select name="cat_{{e.id}}">
              {% for c in categories %}
                <option value="{{c}}" {% if e.category==c %}selected{% endif %}>{{c}}</option>
              {% endfor %}
            </select>
          </td>
          <td class="right"><input type="number" name="amt_{{e.id}}" min="0" value="{{e.amount}}" style="width:120px;"></td>
          <td><input type="text" name="note_{{e.id}}" value="{{e.note or ''}}" style="width:100%;"></td>
        </tr>
      {% endfor %}
      {% if not expenses %}
        <tr><td colspan="4" class="muted">Sin egresos.</td></tr>
      {% endif %}
    </table>

    <h4 style="margin-top:12px;">Agregar egreso</h4>
    <div class="row2">
      <div>
        <select name="new_category">
          <option value="">(no agregar)</option>
          {% for c in categories %}<option value="{{c}}">{{c}}</option>{% endfor %}
        </select>
      </div>
      <div style="display:flex; gap:10px; align-items:center;">
        <input type="number" name="new_amount" min="0" style="width:140px;">
        <input type="text" name="new_note" placeholder="nota (obligatoria si Otros)" style="flex:1;">
      </div>
    </div>
  </div>

  <div class="box">
    <h3>Cierre</h3>
    <div class="row">
      <div>
        <label>Retirado</label><br>
        <input type="number" name="withdrawn" min="0" value="{{close.withdrawn_cash}}" required style="width:100%;">
      </div>
      <div>
        <label>Estado</label><br>
        <select name="ok_status" required style="width:100%;">
          <option value="OK" {% if close.close_ok==1 %}selected{% endif %}>OK</option>
          <option value="NOOK" {% if close.close_ok==0 %}selected{% endif %}>NO OK</option>
        </select>
      </div>
      <div>
        <label>Caja final REAL</label><br>
        <input type="number" name="ending_real" min="0" value="{{close.ending_cash}}" required style="width:100%;">
      </div>
      <div>
        <label>Observación</label><br>
        <input type="text" name="note" value="{{close.note or ''}}" style="width:100%;">
      </div>
    </div>
    <p class="muted">La diferencia se recalcula automáticamente.</p>
  </div>

  <div class="box">
    <h3>Auditoría</h3>
    <div class="row2">
      <div><b>Motivo de edición (obligatorio)</b></div>
      <div><input type="text" name="reason" required placeholder="Ej: se cargó venta luego del cierre" style="width:100%;"></div>
    </div>
  </div>

  <button class="btn">Guardar edición</button>
</form>

</body>
</html>
"""

@app.route("/caja")
@login_required
def caja_index():
    day_obj, d = parse_day_param(request.args.get("d"))

    start_date, end_date = parse_range_params(request.args.get("start"), request.args.get("end"))
    start = start_date.isoformat()
    end = end_date.isoformat()
    turn = request.args.get("turn") or "ALL"
    resp = request.args.get("resp") or "ALL"

    shifts = {s.turn: s for s in Shift.query.filter_by(day=day_obj).all()}

    closes = {}
    can_edit_map = {}
    u = current_user()

    for s in shifts.values():
        if s.status == "CLOSED":
            c = ShiftClose.query.filter_by(shift_id=s.id).first()
            closes[s.id] = c
            can_edit_map[s.id] = bool(c) and can_edit_close(u, c)

    ventas_netas_map = {}
    efectivo_disp_map = {}

    for s in shifts.values():
        if s.status == "CLOSED":
            ventas_netas_map[s.id] = int(calc_ingreso_neto(s))
            c = closes.get(s.id)
            efectivo_disp_map[s.id] = int(s.sales_cash or 0)

    efectivo_total_dia = None
    ingreso_neto_total_dia = None
    if shifts.get("MORNING") and shifts.get("AFTERNOON"):
        if shifts["MORNING"].status == "CLOSED" and shifts["AFTERNOON"].status == "CLOSED":
            efectivo_total_dia = (
                efectivo_disp_map.get(shifts["MORNING"].id, 0) +
                efectivo_disp_map.get(shifts["AFTERNOON"].id, 0)
            )
            ingreso_neto_total_dia = (ventas_netas_map.get(shifts["MORNING"].id, 0) + ventas_netas_map.get(shifts["AFTERNOON"].id, 0))

    responsibles = sorted({
        s.responsible for s in Shift.query.filter(Shift.status == "CLOSED").all()
        if s.responsible
    })

    summary = get_caja_summary(
        limit=400,
        start=start_date,
        end=end_date,
        turn=turn,
        responsible=resp
    )

    return render_template_string(
        CAJA_INDEX_HTML,
        base_css=BASE_CSS,
        d=d,
        turns=TURNS,
        shifts=shifts,
        closes=closes,
        can_edit_map=can_edit_map,
        ventas_netas_map=ventas_netas_map,
        efectivo_disp_map=efectivo_disp_map,
        efectivo_total_dia=efectivo_total_dia,
        ingreso_neto_total_dia=ingreso_neto_total_dia,
        summary=summary,
        db_path=DB_PATH,
        start=start,
        end=end,
        turn=turn,
        resp=resp,
        responsibles=responsibles
    )

@app.route("/caja/open/<turn>", methods=["GET","POST"])
@login_required
def open_shift(turn):
    day_obj, d = parse_day_param(request.args.get("d") or request.form.get("d"))

    existing = Shift.query.filter_by(day=day_obj, turn=turn).first()
    if existing:
        return redirect(url_for("shift", id=existing.id, d=d))

    locked_opening = get_locked_opening_cash(day_obj, turn)
    u = current_user()
    default_responsible = u.username if u else ""

    if request.method == "POST":
        responsible = (request.form.get("responsible") or "").strip()
        opening_cash = safe_int(request.form.get("opening_cash"))

        if not responsible or opening_cash is None:
            return redirect(url_for("open_shift", turn=turn, d=d))

        if locked_opening is not None and int(opening_cash) != int(locked_opening):
            return redirect(url_for("open_shift", turn=turn, d=d))

        s = Shift(day=day_obj, turn=turn, responsible=responsible, opening_cash=int(opening_cash))
        db.session.add(s)
        db.session.commit()
        return redirect(url_for("shift", id=s.id, d=d))

    return render_template_string(
        OPEN_HTML,
        base_css=BASE_CSS,
        d=d,
        day=day_obj.isoformat(),
        turn_name=TURN_NAMES.get(turn, turn),
        locked_opening=locked_opening,
        default_responsible=default_responsible
    )

@app.route("/caja/shift/<int:id>")
@login_required
def shift(id):
    d = request.args.get("d") or date.today().isoformat()
    s = Shift.query.get_or_404(id)

    expenses = CashExpense.query.filter_by(shift_id=id).order_by(CashExpense.created_at.asc()).all()
    close_row = ShiftClose.query.filter_by(shift_id=id).first()

    egresos_total = expenses_total(id)
    ingreso_neto = calc_ingreso_neto(s)
    ingreso_bruto = calc_ingreso_bruto(s, egresos_total)

    efectivo_disponible = int(s.sales_cash or 0)

    u = current_user()
    can_edit = bool(close_row) and can_edit_close(u, close_row)

    return render_template_string(
        SHIFT_HTML,
        base_css=BASE_CSS,
        s=s,
        expenses=expenses,
        close=close_row,
        categories=CATEGORIES,
        turn_name=TURN_NAMES.get(s.turn, s.turn),
        d=d,
        egresos_total=egresos_total,
        ingreso_neto=ingreso_neto,
        ingreso_bruto=ingreso_bruto,
        efectivo_disponible=efectivo_disponible,
        can_edit=can_edit
    )

@app.route("/caja/shift/<int:id>/sales", methods=["POST"])
@login_required
def sales(id):
    d = request.args.get("d") or date.today().isoformat()
    s = Shift.query.get_or_404(id)
    if s.status != "OPEN":
        return redirect(url_for("shift", id=id, d=d))

    s.sales_cash = to_int(request.form.get("sales_cash"))
    s.sales_mp = to_int(request.form.get("sales_mp"))
    s.sales_pya = to_int(request.form.get("sales_pya"))
    s.sales_rappi = to_int(request.form.get("sales_rappi"))

    db.session.commit()
    return redirect(url_for("shift", id=id, d=d))

@app.route("/caja/shift/<int:id>/expense", methods=["POST"])
@login_required
def expense(id):
    d = request.args.get("d") or date.today().isoformat()
    s = Shift.query.get_or_404(id)
    if s.status != "OPEN":
        return redirect(url_for("shift", id=id, d=d))

    category = (request.form.get("category") or "").strip()
    amount = safe_int(request.form.get("amount"))
    note = (request.form.get("note") or "").strip()

    if category not in CATEGORIES or amount is None or amount <= 0:
        return redirect(url_for("shift", id=id, d=d))

    if category.startswith("Otros") and not note:
        return redirect(url_for("shift", id=id, d=d))

    e = CashExpense(shift_id=id, category=category, amount=int(amount), note=note or None)
    db.session.add(e)
    db.session.commit()
    return redirect(url_for("shift", id=id, d=d))

@app.route("/caja/shift/<int:id>/close", methods=["POST"])
@login_required
def close_shift(id):
    d = request.args.get("d") or date.today().isoformat()
    s = Shift.query.get_or_404(id)
    if s.status != "OPEN":
        return redirect(url_for("shift", id=id, d=d))

    withdrawn = safe_int(request.form.get("withdrawn"))
    ok_status = (request.form.get("ok_status") or "").strip()
    if withdrawn is None or ok_status not in ("OK", "NOOK"):
        return redirect(url_for("shift", id=id, d=d))

    ending_calc = calc_ending_calc(cash_bruto(s), withdrawn)

    if ok_status == "OK":
        ending_real = ending_calc
        note = None
        close_ok = 1
    else:
        ending_real = safe_int(request.form.get("ending_real"))
        note = (request.form.get("note") or "").strip()
        close_ok = 0
        if ending_real is None or not note:
            return redirect(url_for("shift", id=id, d=d))

    difference = int(ending_real) - int(ending_calc)

    c = ShiftClose(
        shift_id=id,
        withdrawn_cash=int(withdrawn),
        ending_calc=int(ending_calc),
        ending_cash=int(ending_real),
        difference=int(difference),
        note=note or None,
        close_ok=close_ok,
        edit_count=0
    )
    s.status = "CLOSED"
    s.closed_at = datetime.utcnow()
    db.session.add(c)
    db.session.commit()
    return redirect(url_for("caja_index", d=d))

@app.route("/caja/shift/<int:id>/edit_all", methods=["GET","POST"])
@login_required
def edit_all(id):
    d = request.args.get("d") or date.today().isoformat()
    s = Shift.query.get_or_404(id)
    close_row = ShiftClose.query.filter_by(shift_id=id).first()
    if not close_row or s.status != "CLOSED":
        abort(404)

    u = current_user()
    if not can_edit_close(u, close_row):
        abort(403)

    expenses = CashExpense.query.filter_by(shift_id=id).order_by(CashExpense.created_at.asc()).all()

    if request.method == "POST":
        reason = (request.form.get("reason") or "").strip()
        if not reason:
            return redirect(url_for("edit_all", id=id, d=d))

        responsible = (request.form.get("responsible") or "").strip()
        opening_cash = safe_int(request.form.get("opening_cash"))
        if not responsible or opening_cash is None:
            return redirect(url_for("edit_all", id=id, d=d))

        s.responsible = responsible
        s.opening_cash = int(opening_cash)

        s.sales_cash = to_int(request.form.get("sales_cash"))
        s.sales_mp = to_int(request.form.get("sales_mp"))
        s.sales_pya = to_int(request.form.get("sales_pya"))
        s.sales_rappi = to_int(request.form.get("sales_rappi"))

        for e in expenses:
            if request.form.get(f"del_{e.id}") == "on":
                db.session.delete(e)
                continue

            cat = (request.form.get(f"cat_{e.id}") or "").strip()
            amt = safe_int(request.form.get(f"amt_{e.id}"))
            note = (request.form.get(f"note_{e.id}") or "").strip()

            if cat not in CATEGORIES:
                return redirect(url_for("edit_all", id=id, d=d))
            if amt is None or amt < 0:
                return redirect(url_for("edit_all", id=id, d=d))
            if cat.startswith("Otros") and not note:
                return redirect(url_for("edit_all", id=id, d=d))

            e.category = cat
            e.amount = int(amt)
            e.note = note or None

        new_cat = (request.form.get("new_category") or "").strip()
        new_amt = safe_int(request.form.get("new_amount"))
        new_note = (request.form.get("new_note") or "").strip()
        if new_cat:
            if new_cat not in CATEGORIES:
                return redirect(url_for("edit_all", id=id, d=d))
            if new_amt is None or new_amt <= 0:
                return redirect(url_for("edit_all", id=id, d=d))
            if new_cat.startswith("Otros") and not new_note:
                return redirect(url_for("edit_all", id=id, d=d))
            db.session.add(CashExpense(shift_id=id, category=new_cat, amount=int(new_amt), note=new_note or None))

        db.session.flush()

        withdrawn = safe_int(request.form.get("withdrawn"))
        ok_status = (request.form.get("ok_status") or "").strip()
        ending_real = safe_int(request.form.get("ending_real"))
        note = (request.form.get("note") or "").strip()

        if withdrawn is None or withdrawn < 0 or ok_status not in ("OK", "NOOK") or ending_real is None:
            return redirect(url_for("edit_all", id=id, d=d))

        ending_calc = calc_ending_calc(cash_bruto(s), withdrawn)

        if ok_status == "OK":
            ending_real = ending_calc
            note_to_save = None
            close_ok = 1
        else:
            if not note:
                return redirect(url_for("edit_all", id=id, d=d))
            note_to_save = note
            close_ok = 0

        difference = int(ending_real) - int(ending_calc)

        close_row.withdrawn_cash = int(withdrawn)
        close_row.ending_calc = int(ending_calc)
        close_row.ending_cash = int(ending_real)
        close_row.difference = int(difference)
        close_row.close_ok = close_ok
        close_row.note = note_to_save

        close_row.edit_count = int(close_row.edit_count or 0) + 1
        close_row.edited_by = u.username
        close_row.edited_at = datetime.utcnow()
        close_row.edit_reason = reason

        db.session.commit()
        return redirect(url_for("shift", id=id, d=d))

    return render_template_string(
        EDIT_ALL_HTML,
        base_css=BASE_CSS,
        s=s,
        close=close_row,
        expenses=expenses,
        categories=CATEGORIES,
        user=u,
        d=d,
        turn_name=TURN_NAMES.get(s.turn, s.turn)
    )

@app.route("/caja/export/excel")
@login_required
def export_excel():
    data = get_caja_summary(limit=10000)
    wb = Workbook()
    ws = wb.active
    ws.title = "Cierres"

    ws.append([
        "Fecha","Turno","Responsable",
        "Caja Inicial","Efectivo bruto","Ventas Mercado Pago","Ventas Pedidos Ya","Ventas Rappi",
        "Egresos","Ventas Totales","Ventas Netas","Retirado","Caja Final (Real)","Diferencia"
    ])

    for r in data:
        ws.append([
            r["day"], r["turn_name"], r["responsible"],
            r["opening_cash"], r["sales_cash"], r["sales_mp"], r["sales_pya"], r["sales_rappi"],
            r["expenses"], r["ventas_bruto"], r["ventas_neto"],
            r["withdrawn"], r["ending_real"], r["difference"]
        ])

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    filename = f"cierres_caja_{date.today().isoformat()}.xlsx"
    return send_file(bio, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.route("/caja/export/pdf")
@login_required
def export_pdf():
    data = get_caja_summary(limit=2000)
    bio = BytesIO()
    c = canvas.Canvas(bio, pagesize=landscape(A4))
    width, height = landscape(A4)

    c.setFont("Helvetica-Bold", 14)
    c.drawString(30, height - 30, "PORA - Resumen de cierres de caja")
    c.setFont("Helvetica", 9)

    y = height - 55
    headers = ["Fecha","Turno","Resp","CI","Efe","MP","PYa","Rappi","Egr","Bruto","Neto","Ret","Final","Diff"]
    col_widths = [70, 60, 80, 55, 55, 60, 55, 55, 55, 55, 55, 55, 60, 55]
    x0 = 16

    def draw_row(values, y_):
        x = x0
        for v, w in zip(values, col_widths):
            c.drawString(x, y_, str(v))
            x += w

    c.setFont("Helvetica-Bold", 9)
    draw_row(headers, y)
    c.line(16, y - 3, width - 16, y - 3)
    y -= 16

    c.setFont("Helvetica", 9)
    for r in data:
        if y < 40:
            c.showPage()
            c.setFont("Helvetica-Bold", 14)
            c.drawString(30, height - 30, "PORA - Resumen de cierres de caja (cont.)")
            c.setFont("Helvetica", 9)
            y = height - 55
            c.setFont("Helvetica-Bold", 9)
            draw_row(headers, y)
            c.line(16, y - 3, width - 16, y - 3)
            y -= 16
            c.setFont("Helvetica", 9)

        draw_row([
            r["day"], r["turn_name"], (r["responsible"] or "")[:10],
            r["opening_cash"], r["sales_cash"], r["sales_mp"], r["sales_pya"], r["sales_rappi"],
            r["expenses"], r["ventas_bruto"], r["ventas_neto"],
            r["withdrawn"], r["ending_real"], r["difference"]
        ], y)
        y -= 14

    c.save()
    bio.seek(0)
    filename = f"cierres_caja_{date.today().isoformat()}.pdf"
    return send_file(bio, as_attachment=True, download_name=filename, mimetype="application/pdf")

# ==============================
# IMPORT CAJA (Opción A)
# ==============================
IMPORT_CAJA_HTML = """
<!doctype html>
<html lang="es">
<head><meta charset="utf-8"><title>Import Caja</title>{{ base_css|safe }}</head>
<body style="max-width:950px;">
  <a href="{{ url_for('caja_index') }}">← Volver</a>
  <h2>Importar datos (Excel)</h2>

  <div class="box" style="margin-bottom:12px; border-color:#ffbf66;background:#fff0d6;">
    <b>Formato:</b> hojas <code>shifts</code> y <code>expenses</code>.<br>
    Se importan turnos y egresos, y el turno queda <b>CERRADO</b>.<br>
    <b>Regla aplicada:</b> Retirado = Efectivo neto (Efectivo bruto - Caja inicial).
  </div>

  {% if msg %}<div class="box" style="border-color:#7ad2a4;background:#dff5e6;"><b>{{msg}}</b></div>{% endif %}
  {% if err %}<div class="box" style="border-color:#ffb866;background:#ffe6d6;"><b>Error:</b> {{err}}</div>{% endif %}

  <form method="post" enctype="multipart/form-data" style="margin-top:12px;">
    <div class="box">
      <label><b>Archivo Excel</b></label><br>
      <input type="file" name="file" accept=".xlsx" required>
      <br><br>
      <label><b>Modo</b></label><br>
      <select name="mode">
        <option value="skip" selected>Si existe el turno, NO tocarlo (skip)</option>
        <option value="replace">Si existe el turno, REEMPLAZAR (replace)</option>
      </select>
      <div class="muted" style="margin-top:6px;">Tip: para “pisar todo”, usá modo <b>replace</b>.</div>
    </div>
    <br>
    <button class="btn">Importar</button>
  </form>
</body>
</html>
"""

def _excel_to_date(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except:
        pass
    try:
        dd, mm, yy = s.split("/")
        return date(int(yy), int(mm), int(dd))
    except:
        return None

@app.route("/import/caja", methods=["GET","POST"])
@login_required
def import_caja():
    err = None
    msg = None

    if request.method == "POST":
        mode = (request.form.get("mode") or "skip").strip()
        f = request.files.get("file")
        if not f:
            err = "No se recibió archivo."
        else:
            try:
                wb = load_workbook(f, data_only=True)
                if "shifts" not in wb.sheetnames:
                    raise ValueError("Error: Falta la hoja 'shifts'.")
                if "expenses" not in wb.sheetnames:
                    raise ValueError("Error: Falta la hoja 'expenses'.")

                ws_s = wb["shifts"]
                ws_e = wb["expenses"]

                rows_s = list(ws_s.iter_rows(values_only=True))
                if len(rows_s) < 2:
                    raise ValueError("Error: La hoja 'shifts' no tiene datos.")

                headers_s = [str(x).strip() if x is not None else "" for x in rows_s[0]]
                need_s = ["day","turn","responsible","opening_cash","sales_cash","sales_mp","sales_pya","sales_rappi"]
                for n in need_s:
                    if n not in headers_s:
                        raise ValueError(f"Error: Falta columna '{n}' en shifts.")
                idxs = {k:i for i,k in enumerate(headers_s)}

                rows_e = list(ws_e.iter_rows(values_only=True))
                headers_e = [str(x).strip() if x is not None else "" for x in rows_e[0]] if rows_e else []
                need_e = ["day","turn","category","amount","note"]
                for n in need_e:
                    if n not in headers_e:
                        raise ValueError(f"Error: Falta columna '{n}' en expenses.")
                idxe = {k:i for i,k in enumerate(headers_e)}

                imported = 0
                replaced = 0

                exp_map = {}
                for r in rows_e[1:]:
                    if not r or all(v is None or str(v).strip()=="" for v in r):
                        continue
                    dd = _excel_to_date(r[idxe["day"]])
                    tt = (str(r[idxe["turn"]] or "").strip().upper())
                    if tt not in ("MORNING","AFTERNOON"):
                        continue
                    cat = (str(r[idxe["category"]] or "").strip())
                    amt = int(r[idxe["amount"]] or 0)
                    note = (str(r[idxe["note"]] or "").strip() or None)
                    exp_map.setdefault((dd, tt), []).append((cat, amt, note))

                for r in rows_s[1:]:
                    if not r or all(v is None or str(v).strip()=="" for v in r):
                        continue

                    dd = _excel_to_date(r[idxs["day"]])
                    tt = (str(r[idxs["turn"]] or "").strip().upper())
                    if dd is None:
                        continue
                    if tt not in ("MORNING","AFTERNOON"):
                        continue

                    responsible = str(r[idxs["responsible"]] or "").strip() or "Bernardo"
                    opening_cash = int(r[idxs["opening_cash"]] or 0)
                    sales_cash = int(r[idxs["sales_cash"]] or 0)
                    sales_mp = int(r[idxs["sales_mp"]] or 0)
                    sales_pya = int(r[idxs["sales_pya"]] or 0)
                    sales_rappi = int(r[idxs["sales_rappi"]] or 0)

                    existing = Shift.query.filter_by(day=dd, turn=tt).first()
                    if existing:
                        if mode == "skip":
                            continue
                        c = ShiftClose.query.filter_by(shift_id=existing.id).first()
                        if c:
                            db.session.delete(c)
                        CashExpense.query.filter_by(shift_id=existing.id).delete()
                        db.session.delete(existing)
                        db.session.flush()
                        replaced += 1

                    s = Shift(
                        day=dd, turn=tt, responsible=responsible,
                        opening_cash=opening_cash,
                        sales_cash=sales_cash, sales_mp=sales_mp,
                        sales_pya=sales_pya, sales_rappi=sales_rappi,
                        status="CLOSED", closed_at=datetime.utcnow()
                    )
                    db.session.add(s)
                    db.session.flush()

                    for (cat, amt, note) in exp_map.get((dd, tt), []):
                        if cat not in CATEGORIES:
                            cat = "Delivery / Cadete (efectivo)"
                        if cat.startswith("Otros") and not note:
                            note = "import"
                        if amt and amt > 0:
                            db.session.add(CashExpense(shift_id=s.id, category=cat, amount=int(amt), note=note))

                    withdrawn = max(0, int(sales_cash) - int(opening_cash))
                    ending_calc = calc_ending_calc(sales_cash, withdrawn)
                    ending_real = ending_calc
                    diff = 0

                    db.session.add(ShiftClose(
                        shift_id=s.id,
                        withdrawn_cash=withdrawn,
                        ending_calc=ending_calc,
                        ending_cash=ending_real,
                        difference=diff,
                        note=None,
                        close_ok=1,
                        edit_count=0
                    ))

                    imported += 1

                db.session.commit()
                msg = f"Import OK. Turnos importados: {imported}. Reemplazados: {replaced}."

            except Exception as ex:
                db.session.rollback()
                err = str(ex)

    return render_template_string(IMPORT_CAJA_HTML, base_css=BASE_CSS, err=err, msg=msg)

# ============================================================
# =====================  ASISTENCIA (UI)  =====================
# ============================================================

ATT_CONFIG_HTML = """
<!doctype html>
<html lang="es">
<head><meta charset="utf-8"><title>Config Asistencia</title>{{ base_css|safe }}</head>
<body style="max-width:1000px;">
  <a href="{{ url_for('asistencia') }}">← Volver</a>
  <h2>Configurar grupos / vacaciones</h2>

  <div class="box">
    <b>Horarios por grupo</b><br>
    <span class="muted">
      <b>Grupo A:</b> {{ga.morning_in}}–{{ga.morning_out}} / {{ga.afternoon_in}}–{{ga.afternoon_out}}<br>
      <b>Grupo B:</b> {{gb.morning_in}}–{{gb.morning_out}} / {{gb.afternoon_in}}–{{gb.afternoon_out}}
    </span>
  </div>

  {% if msg %}<div class="box" style="border-color:#7ad2a4;background:#dff5e6;"><b>{{msg}}</b></div>{% endif %}
  {% if err %}<div class="box" style="border-color:#ffb866;background:#ffe6d6;"><b>Error:</b> {{err}}</div>{% endif %}

  <h3>Rotación semanal (A/B)</h3>
  <div class="box">
    <p class="muted">
      Rotación automática semanal (A↔B).<br>
      {% if cfg_exists %}
        <b>Edición:</b> solo Admin puede modificar configuración existente.
      {% else %}
        <b>Inicial:</b> cualquier usuario puede configurar por primera vez.
      {% endif %}
    </p>
    <form method="post">
      <input type="hidden" name="action" value="set_rotation">
      <table>
        <tr><th>Empleado</th><th>Grupo (Semana base)</th></tr>
        {% for e in employees %}
        <tr>
          <td><b>{{e}}</b></td>
          <td>
            <select name="g_{{e}}" {% if lock_rotation %}disabled{% endif %}>
              <option value="A" {% if week0.get(e,'A')=='A' %}selected{% endif %}>A</option>
              <option value="B" {% if week0.get(e,'A')=='B' %}selected{% endif %}>B</option>
            </select>
          </td>
        </tr>
        {% endfor %}
      </table>
      <br>
      <button class="btn" {% if lock_rotation %}disabled{% endif %}>Guardar rotación</button>
      {% if lock_rotation %}
        <div class="muted">Solo Admin puede editar esta configuración.</div>
      {% endif %}
    </form>
  </div>

  <h3>Vacaciones (pre-carga)</h3>
  <div class="box">
    <form method="post" style="display:flex; gap:10px; flex-wrap:wrap; align-items:end;">
      <input type="hidden" name="action" value="add_vac">
      <div>
        <label class="muted">Empleado</label><br>
        <select name="emp">
          {% for e in employees %}<option value="{{e}}">{{e}}</option>{% endfor %}
        </select>
      </div>
      <div>
        <label class="muted">Desde</label><br>
        <input type="date" name="start" required>
      </div>
      <div>
        <label class="muted">Hasta</label><br>
        <input type="date" name="end" required>
      </div>
      <div>
        <button class="btn">Agregar</button>
      </div>
    </form>

    <table style="margin-top:12px;">
      <tr><th>Empleado</th><th>Desde</th><th>Hasta</th><th>Acción</th></tr>
      {% for v in vacations %}
        <tr>
          <td>{{v.employee}}</td>
          <td>{{v.start_day}}</td>
          <td>{{v.end_day}}</td>
          <td>
            <form method="post" style="display:inline;">
              <input type="hidden" name="action" value="del_vac">
              <input type="hidden" name="id" value="{{v.id}}">
              <button class="btn">Borrar</button>
            </form>
          </td>
        </tr>
      {% endfor %}
      {% if not vacations %}
        <tr><td colspan="4" class="muted">Sin vacaciones configuradas.</td></tr>
      {% endif %}
    </table>
  </div>

</body>
</html>
"""

@app.route("/asistencia/config", methods=["GET","POST"])
@login_required
def att_config():
    u = current_user()
    err = None
    msg = None

    cfg = rotation_config_get()
    cfg_exists = bool(cfg and cfg.week0_map)
    week0 = parse_week0_map(cfg.week0_map) if cfg_exists else {}

    lock_rotation = cfg_exists and (u.role != "admin")

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        try:
            if action == "set_rotation":
                if lock_rotation:
                    raise ValueError("Solo Admin puede editar la rotación una vez creada.")
                m = {}
                for e in EMPLOYEES:
                    g = (request.form.get(f"g_{e}") or "A").strip().upper()
                    if g not in ("A","B"):
                        g = "A"
                    m[e] = g
                if not cfg:
                    cfg = RotationConfig(week0_map=make_week0_map_str(m), created_by=u.username)
                    db.session.add(cfg)
                else:
                    cfg.week0_map = make_week0_map_str(m)
                    cfg.created_by = cfg.created_by or u.username
                db.session.commit()
                msg = "Rotación guardada."
            elif action == "add_vac":
                emp = (request.form.get("emp") or "").strip()
                start = date.fromisoformat(request.form.get("start"))
                end = date.fromisoformat(request.form.get("end"))
                if emp not in EMPLOYEES:
                    raise ValueError("Empleado inválido.")
                if start > end:
                    start, end = end, start
                db.session.add(Vacation(employee=emp, start_day=start, end_day=end))
                db.session.commit()
                msg = "Vacaciones agregadas."
            elif action == "del_vac":
                vid = int(request.form.get("id") or 0)
                v = Vacation.query.get(vid)
                if v:
                    db.session.delete(v)
                    db.session.commit()
                msg = "Vacaciones borradas."
        except Exception as ex:
            db.session.rollback()
            err = str(ex)

    vacations = Vacation.query.order_by(Vacation.employee.asc(), Vacation.start_day.asc()).all()

    return render_template_string(
        ATT_CONFIG_HTML,
        base_css=BASE_CSS,
        ga=GROUP_A, gb=GROUP_B,
        employees=EMPLOYEES,
        cfg_exists=cfg_exists,
        lock_rotation=lock_rotation,
        week0=week0,
        vacations=vacations,
        err=err,
        msg=msg
    )

ASISTENCIA_HTML = """
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Asistencia</title>
{{ base_css|safe }}
<style>
  body{max-width:1500px;}
  th,td{padding:8px;}
  .grid{display:flex; gap:10px; flex-wrap:wrap; align-items:end; margin:12px 0;}
  details{background:#fafafa;border:1px solid #ddd;border-radius:10px;padding:8px;}
  summary{cursor:pointer;font-weight:bold;}
  .w-time{width:110px;}
  .w-emp{width:120px;}
  .w-group{width:120px;}
  .w-nov{width:220px;}
  .w-notes{width:260px;}
  .filters{display:flex; gap:10px; flex-wrap:wrap; align-items:end; margin:12px 0;}
  .holiday-bg{background:#ffe3ec;}
  .holiday-bg table tr{background:#fff;}
</style>
</head>
<body class="{{ 'holiday-bg' if hday else '' }}">

<div class="top">
  <div>
    <h2 style="margin:0;">Asistencia</h2>
    <div class="muted"><a href="{{ url_for('home') }}">← Volver al menú</a></div>
  </div>
  <div class="row">
    <a class="btn" href="{{ url_for('att_export_excel', emp=emp, start=start, end=end, novf=novf) }}">Exportar Excel</a>
    <a class="btn" href="{{ url_for('att_export_pdf', emp=emp, start=start, end=end, novf=novf) }}">Exportar PDF</a>
    <a class="btn" href="{{ url_for('import_asistencia') }}">Importar Excel</a>
    <a class="btn" href="{{ url_for('att_config') }}">Configurar grupos / vacaciones</a>
  </div>
</div>

<div class="box" style="margin-top:10px;">
  <b>Horarios por grupo</b><br>
  <span class="muted">
    <b>Grupo A:</b> {{ga.morning_in}}–{{ga.morning_out}} / {{ga.afternoon_in}}–{{ga.afternoon_out}} &nbsp;&nbsp;|&nbsp;&nbsp;
    <b>Grupo B:</b> {{gb.morning_in}}–{{gb.morning_out}} / {{gb.afternoon_in}}–{{gb.afternoon_out}}
  </span>
</div>

<form method="get" action="{{ url_for('asistencia') }}" class="grid">
  <div>
    <label class="muted">Fecha:</label><br>
    <input type="date" name="d" value="{{d}}">
  </div>
  <div>
    <button class="btn" type="submit">Ir</button>
  </div>
</form>

<form method="post" action="{{ url_for('asistencia') }}" class="grid" style="margin-top:6px;">
  <input type="hidden" name="action" value="set_holiday">
  <input type="hidden" name="d" value="{{d}}">
  <div>
    <label class="muted">Feriado:</label><br>
    <select name="hday" style="min-width:220px;">
      <option value="" {% if hday=='' %}selected{% endif %}>-</option>
      <option value="LABORABLE" {% if hday=='LABORABLE' %}selected{% endif %}>Feriado Laborable</option>
      <option value="NO_LABORABLE" {% if hday=='NO_LABORABLE' %}selected{% endif %}>Feriado No laborable (se paga el día)</option>
    </select>
  </div>
  <div>
    <button class="btn" type="submit">Aplicar</button>
  </div>
</form>

{% if err %}<div class="box" style="border-color:#ffb866;background:#ffe6d6;"><b>Error:</b> {{err}}</div>{% endif %}
{% if msg %}<div class="box" style="border-color:#7ad2a4;background:#dff5e6;"><b>{{msg}}</b></div>{% endif %}

<form method="post">
<input type="hidden" name="d" value="{{d}}">

<table>
  <tr>
    <th class="w-emp">Empleado</th>
    <th class="w-group">Grupo</th>
    <th colspan="3">Mañana</th>
    <th colspan="3">Tarde</th>
    <th>Horas (paga)</th>
    <th class="w-nov">Novedad</th>
    <th class="w-notes">Notas</th>
    <th>Consumos</th>
  </tr>
  <tr>
    <th></th>
    <th></th>
    <th class="w-time">Ing</th><th class="w-time">Egr</th><th>Horas</th>
    <th class="w-time">Ing</th><th class="w-time">Egr</th><th>Horas</th>
    <th></th>
    <th></th><th></th><th></th>
  </tr>

  {% for r in rows %}
  <tr class="{% if r.is_vac %}vac-row{% elif r.has_warning %}warn-row{% endif %}">
    <td>
      <b>{{r.employee}}</b><br>
      {% if r.chip %}
        <span class="chip {{r.chip_class}}">{{r.chip}}</span>
      {% endif %}
    </td>

    <td>
      <select name="{{r.key}}_mode">
        <option value="AUTO" {% if r.mode=='AUTO' %}selected{% endif %}>Auto</option>
        <option value="MANUAL" {% if r.mode=='MANUAL' %}selected{% endif %}>Manual</option>
      </select>
      <div class="muted">Sug: {{r.group}}</div>
      <input type="hidden" name="{{r.key}}_group" value="{{r.group}}">
    </td>

    <td><input class="w-time {{'preset' if r.mi_preset else ''}}" type="time" name="{{r.key}}_mi" value="{{r.morning_in}}" {% if r.lock_times %}readonly{% endif %}></td>
    <td><input class="w-time {{'preset' if r.mo_preset else ''}}" type="time" name="{{r.key}}_mo" value="{{r.morning_out}}" {% if r.lock_times %}readonly{% endif %}></td>
    <td>{{r.morning_time}}</td>

    <td><input class="w-time {{'preset' if r.ai_preset else ''}}" type="time" name="{{r.key}}_ai" value="{{r.afternoon_in}}" {% if r.lock_times %}readonly{% endif %}></td>
    <td><input class="w-time {{'preset' if r.ao_preset else ''}}" type="time" name="{{r.key}}_ao" value="{{r.afternoon_out}}" {% if r.lock_times %}readonly{% endif %}></td>
    <td>{{r.afternoon_time}}</td>

    <td><b>{{r.payable_time}}</b><div class="muted">{{r.warn_text}}</div></td>

    <td>
      <select name="{{r.key}}_nov" onchange="toggleNovelty('{{r.key}}');">
        {% for it in novelty_items %}
          <option value="{{it}}" {% if r.novelty==it %}selected{% endif %}>{{it if it else "-"}}</option>
        {% endfor %}
      </select>

      <div id="{{r.key}}_nov_extra" style="margin-top:6px; display:none;">
        <div class="muted" id="{{r.key}}_nov_hint"></div>
        <input type="text" name="{{r.key}}_novx" id="{{r.key}}_novx" style="width:140px;" placeholder="">
      </div>

      <span data-key="{{r.key}}" data-nov="{{r.novelty}}" data-novm="{{r.novelty_minutes}}" style="display:none;"></span>
    </td>

    <td><input style="width:100%;" name="{{r.key}}_notes" value="{{r.notes or ''}}" placeholder="detalle opcional"></td>

    <td>
      <details>
        <summary>Ver / Editar</summary>
        <div class="muted" style="margin-top:8px;">Cantidad:</div>
        <select name="{{r.key}}_cq" id="{{r.key}}_cq" onchange="setRows('{{r.key}}')">
          {% for i in range(0, maxc+1) %}
            <option value="{{i}}" {% if r.cons_count==i %}selected{% endif %}>{{i}}</option>
          {% endfor %}
        </select>

        <table style="margin-top:8px;">
          <tr><th>#</th><th>Consumo</th><th>Monto</th></tr>
          {% for i in range(1, maxc+1) %}
          <tr class="{{r.key}}_row" data-i="{{i}}">
            <td>{{i}}</td>
            <td><input style="width:260px;" type="text" name="{{r.key}}_ci{{i}}" value="{{ r.cons[i-1].item if r.cons|length>=i else '' }}"></td>
            <td><input style="width:120px;" type="number" min="0" name="{{r.key}}_cm{{i}}" value="{{ r.cons[i-1].amount if r.cons|length>=i and r.cons[i-1].amount is not none else '' }}"></td>
          </tr>
          {% endfor %}
        </table>
      </details>
    </td>
  </tr>
  {% endfor %}
</table>

<br>
<button class="btn">Guardar</button>
</form>

<hr>

<h2>Resumen / Filtros</h2>

<div class="row" style="margin:10px 0; gap:10px;">
  <div class="box" style="min-width:220px;"><b>Horas totales:</b> {{ sum_hours_txt }}</div>
  <div class="box" style="min-width:220px;"><b>Consumos totales:</b> {{ sum_cons_money|money }}</div>
</div>


<form method="get" action="{{ url_for('asistencia') }}" class="filters">
  <input type="hidden" name="d" value="{{d}}">

  <div>
    <label class="muted">Empleado:</label><br>
    <select name="emp">
      <option value="ALL" {% if emp=='ALL' %}selected{% endif %}>Todos</option>
      {% for e in employees %}
        <option value="{{e}}" {% if emp==e %}selected{% endif %}>{{e}}</option>
      {% endfor %}
    </select>
  </div>

  <div>
    <label class="muted">Desde:</label><br>
    <input type="date" name="start" value="{{start}}">
  </div>

  <div>
    <label class="muted">Hasta:</label><br>
    <input type="date" name="end" value="{{end}}">
  </div>

  <div>
    <label class="muted">Novedad:</label><br>
    <select name="novf">
      <option value="ALL" {% if novf=='ALL' %}selected{% endif %}>Todas</option>
      {% for it in novelty_items %}
        {% if it %}
          <option value="{{it}}" {% if novf==it %}selected{% endif %}>{{it}}</option>
        {% endif %}
      {% endfor %}
    </select>
  </div>

  <div>
    <button class="btn" type="submit">Aplicar</button>
  </div>
</form>

<table>
  <tr>
    <th>Fecha</th>
    <th>Empleado</th>
    <th>Grupo</th>
    <th>Mañana</th>
    <th>Tarde</th>
    <th>Horas pagas</th>
    <th>Novedad</th>
    <th>Notas</th>
    <th>Consumos</th>
  </tr>
  {% for r in summary %}
  <tr>
    <td>{{r.day}}</td>
    <td>{{r.employee}}</td>
    <td>{{r.group}}</td>
    <td>{{r.morning}}</td>
    <td>{{r.afternoon}}</td>
    <td><b>{{r.payable}}</b></td>
    <td>{{r.novelty}}</td>
    <td>{{r.notes}}</td>
    <td>
      <b>{{r.cons_total|money}}</b>
      {% if r.cons_items %}
        <div class="muted">{{r.cons_items}}</div>
      {% endif %}
    </td>
  </tr>
  {% endfor %}
  {% if not summary %}
    <tr><td colspan="9" class="muted">Sin registros en el rango.</td></tr>
  {% endif %}
</table>

<script>
  function setRows(key){
    const n = parseInt(document.getElementById(key + "_cq").value || "0", 10);
    const rows = document.querySelectorAll("tr." + key + "_row");
    rows.forEach(r => {
      const i = parseInt(r.getAttribute("data-i"), 10);
      r.style.display = (i <= n) ? "table-row" : "none";
    });
  }

  function toggleNovelty(key){
    const sel = document.querySelector("select[name='"+key+"_nov']");
    const box = document.getElementById(key+"_nov_extra");
    const hint = document.getElementById(key+"_nov_hint");
    const inp = document.getElementById(key+"_novx");

    const v = sel.value || "";
    if (v === "Razones particulares") {
      box.style.display = "block";
      hint.textContent = "Cargar minutos tomados hoy (ej: 30).";
      inp.placeholder = "minutos";
      inp.disabled = false;
    } else if (v === "Enfermedad") {
      box.style.display = "block";
      hint.textContent = "Cargar horas de enfermedad hoy (ej: 2.5).";
      inp.placeholder = "horas";
      inp.disabled = false;
    } else {
      box.style.display = "none";
      inp.disabled = true;
      inp.value = "";
    }
  }

  window.addEventListener("load", () => {
    document.querySelectorAll("select[id$='_cq']").forEach(sel => {
      const key = sel.id.replace("_cq","");
      setRows(key);
    });

    document.querySelectorAll("select[name$='_nov']").forEach(sel => {
      const key = sel.name.replace("_nov","");
      toggleNovelty(key);
    });

    document.querySelectorAll("span[data-novm]").forEach(sp => {
      const key = sp.getAttribute("data-key");
      const nov = sp.getAttribute("data-nov") || "";
      const novm = parseInt(sp.getAttribute("data-novm") || "0", 10);
      const inp = document.getElementById(key+"_novx");
      if (!inp) return;

      if (nov === "Razones particulares") {
        inp.value = String(novm || 0);
      } else if (nov === "Enfermedad") {
        inp.value = String((novm || 0) / 60.0);
      }
    });
  });
</script>

</body>
</html>
"""

def attendance_summary_rows(start: date, end: date, employee: str, novf: str):
    q = Attendance.query.filter(Attendance.day >= start, Attendance.day <= end)
    if employee and employee != "ALL":
        q = q.filter(Attendance.employee == employee)
    # NO filtramos por novf acá: Vacaciones puede venir de tabla Vacation
    q = q.order_by(Attendance.day.desc(), Attendance.employee.asc())

    rows = []
    for a in q.all():
        vac = is_vacation(a.employee, a.day) or (a.novelty == "Vacaciones")
        novelty_display = "Vacaciones" if vac else (a.novelty or "")

        if novf and novf != "ALL":
            if novelty_display != novf:
                continue

        g = a.group_code or group_for_employee_on_day(a.employee, a.day)
        exp = expected_times_for_group(g, a.day)

        mi = a.morning_in or exp["morning_in"]
        mo = a.morning_out or exp["morning_out"]
        ai = a.afternoon_in or exp["afternoon_in"]
        ao = a.afternoon_out or exp["afternoon_out"]

        calc = compute_work_minutes_and_flags(a)
        cons_total, cons_items = consumptions_summary_for_attendance(a.id)

        rows.append({
            "day": a.day.isoformat(),
            "employee": a.employee,
            "group": g,
            "morning": f"{mi}–{mo}".strip("–"),
            "afternoon": f"{ai}–{ao}".strip("–"),
            "payable": fmt_minutes(calc["payable_min"]),
            "novelty": novelty_display,
            "notes": a.notes or "",
            "cons_total": cons_total,
            "cons_items": cons_items,
        })
    return rows

@app.route("/asistencia", methods=["GET","POST"])
@login_required
def asistencia():
    day_obj, d = parse_day_param(request.args.get("d") or request.form.get("d"))

    err = None
    msg = None

    # filtros de resumen
    empf = request.args.get("emp") or "ALL"
    novf = request.args.get("novf") or "ALL"
    start_date, end_date = parse_range_params(request.args.get("start"), request.args.get("end"))
    start = start_date.isoformat()
    end = end_date.isoformat()

    # feriado guardado para el día
    hday = get_holiday_type(day_obj)

    # ======= Aplicar feriado (POST) =======
    if request.method == "POST" and (request.form.get("action") or "").strip() == "set_holiday":
        try:
            h = (request.form.get("hday") or "").strip().upper()
            if h not in ("", "LABORABLE", "NO_LABORABLE"):
                h = ""
            set_holiday_type(day_obj, h)
            db.session.commit()
            msg = "Feriado aplicado."
            return redirect(url_for("asistencia", d=d, emp=empf, start=start, end=end, novf=novf))
        except Exception as ex:
            db.session.rollback()
            err = str(ex)

    # ======= Guardar asistencia (POST) =======
    if request.method == "POST" and err is None:
        try:
            for emp_name in EMPLOYEES:
                key = emp_key(emp_name)
                row = Attendance.query.filter_by(day=day_obj, employee=emp_name).first()
                if not row:
                    row = Attendance(day=day_obj, employee=emp_name, mode="AUTO")
                    db.session.add(row)
                    db.session.flush()

                mode = (request.form.get(f"{key}_mode") or "AUTO").strip().upper()
                if mode not in ("AUTO","MANUAL"):
                    mode = "AUTO"

                g = (request.form.get(f"{key}_group") or "").strip().upper()
                if g not in ("A","B"):
                    g = group_for_employee_on_day(emp_name, day_obj)

                row.mode = mode
                row.group_code = g

                exp = expected_times_for_group(g, day_obj)
                vac = is_vacation(emp_name, day_obj)

                nov = (request.form.get(f"{key}_nov") or "").strip()
                if nov not in NOVELTY_ITEMS:
                    nov = ""
                row.novelty = nov or None

                def get_post_time(field_name: str) -> Optional[str]:
                    v = (request.form.get(field_name) or "").strip()
                    if v and not valid_time_str(v):
                        raise ValueError(f"Hora inválida en {emp_name}: {v}")
                    return v or None

                mi_post = get_post_time(f"{key}_mi")
                mo_post = get_post_time(f"{key}_mo")
                ai_post = get_post_time(f"{key}_ai")
                ao_post = get_post_time(f"{key}_ao")
                if vac or nov == "Vacaciones":
                    # Vacaciones: se fija el horario esperado completo (según grupo/día)
                    row.morning_in = exp.get("morning_in")
                    row.morning_out = exp.get("morning_out")
                    row.afternoon_in = exp.get("afternoon_in")
                    row.afternoon_out = exp.get("afternoon_out")

                elif nov == "Delivery":
                    # Delivery: pago aparte, no computa horas. No permitir carga de horarios.
                    row.morning_in = None
                    row.morning_out = None
                    row.afternoon_in = None
                    row.afternoon_out = None

                else:
                    if mode == "AUTO":
                        # Auto: precarga valores esperados como fallback (gris), y se sobreescriben si se editan.
                        row.morning_in = mi_post or row.morning_in or exp.get("morning_in")
                        row.morning_out = mo_post or row.morning_out or exp.get("morning_out")
                        row.afternoon_in = ai_post or row.afternoon_in or exp.get("afternoon_in")
                        row.afternoon_out = ao_post or row.afternoon_out or exp.get("afternoon_out")
                    else:
                        # Manual: se guarda exactamente lo cargado (puede quedar vacío)
                        row.morning_in = mi_post
                        row.morning_out = mo_post
                        row.afternoon_in = ai_post
                        row.afternoon_out = ao_post


                novx = (request.form.get(f"{key}_novx") or "").strip()
                if nov == "Razones particulares":
                    m = safe_int(novx) or 0
                    row.novelty_minutes = max(0, int(m))
                elif nov == "Enfermedad":
                    h = safe_float(novx) or 0.0
                    mins = int(round(max(0.0, h) * 60.0))
                    row.novelty_minutes = mins
                else:
                    row.novelty_minutes = 0

                row.notes = (request.form.get(f"{key}_notes") or "").strip() or None

                # Validaciones de jornada (solo si no es vacaciones/inasistencia)
                calc = compute_work_minutes_and_flags(row)
                jornada_min = jornada_min_for_day(day_obj)

                if not (vac or nov == "Vacaciones" or nov == "Inasistencia"):
                    if calc["total_worked_min"] < jornada_min and not (row.novelty or ""):
                        raise ValueError(
                            f"{emp_name}: si trabajó menos de {fmt_minutes(jornada_min)} debe cargar Novedad."
                        )
                    if calc["total_worked_min"] > jornada_min and not (row.notes and row.notes.strip()):
                        raise ValueError(
                            f"{emp_name}: si trabajó más de {fmt_minutes(jornada_min)} debe justificar en Notas."
                        )

                db.session.flush()

                # Consumptions (recrear)
                AttendanceConsumption.query.filter_by(attendance_id=row.id).delete()
                qty = safe_int(request.form.get(f"{key}_cq")) or 0
                qty = max(0, min(qty, MAX_CONSUMOS))
                for i in range(1, qty + 1):
                    item = (request.form.get(f"{key}_ci{i}") or "").strip() or None
                    amount = safe_int(request.form.get(f"{key}_cm{i}"))
                    db.session.add(AttendanceConsumption(attendance_id=row.id, idx=i, item=item, amount=amount))

            db.session.commit()
            msg = "Guardado."
            return redirect(url_for("asistencia", d=d, emp=empf, start=start, end=end, novf=novf))

        except Exception as ex:
            db.session.rollback()
            err = str(ex)

    # refrescar feriado guardado (por si se modificó)
    hday = get_holiday_type(day_obj)

    rows = []
    auto_fix_commit = False
    for emp_name in EMPLOYEES:
        key = emp_key(emp_name)
        row = Attendance.query.filter_by(day=day_obj, employee=emp_name).first()
        if not row:
            row = Attendance(day=day_obj, employee=emp_name, mode="AUTO")
            db.session.add(row)
            db.session.flush()
            db.session.commit()

        g = row.group_code or group_for_employee_on_day(emp_name, day_obj)
        exp = expected_times_for_group(g, day_obj)
        # Si viene en MANUAL pero no tiene diferencias respecto al horario esperado,
        # lo volvemos a AUTO para que se vea como 'precargado' (gris) por defecto.
        if (row.mode or '').upper() == 'MANUAL':
            # no tocar si hay novedad / notas / consumos
            cons_cnt = AttendanceConsumption.query.filter_by(attendance_id=row.id).count()
            if (not (row.novelty or '').strip()) and (not (row.notes or '').strip()) and cons_cnt == 0:
                def _eq(a_, b_):
                    return (a_ or '') == (b_ or '')
                if (_eq(row.morning_in, exp.get('morning_in')) and _eq(row.morning_out, exp.get('morning_out')) and
                    _eq(row.afternoon_in, exp.get('afternoon_in')) and _eq(row.afternoon_out, exp.get('afternoon_out'))):
                    row.mode = 'AUTO'
                    auto_fix_commit = True

        vac = is_vacation(emp_name, day_obj) or (row.novelty == "Vacaciones")

        mi_preset = (row.mode or "AUTO") == "AUTO" and (row.morning_in is None) and not vac and bool(exp.get("morning_in"))
        mo_preset = (row.mode or "AUTO") == "AUTO" and (row.morning_out is None) and not vac and bool(exp.get("morning_out"))
        ai_preset = (row.mode or "AUTO") == "AUTO" and (row.afternoon_in is None) and not vac and bool(exp.get("afternoon_in"))
        ao_preset = (row.mode or "AUTO") == "AUTO" and (row.afternoon_out is None) and not vac and bool(exp.get("afternoon_out"))

        def show_time(v: Optional[str], default: str) -> str:
            if vac:
                return default or ""
            if (row.mode or "AUTO") == "AUTO":
                return v or (default or "")
            return v or ""

        mi = show_time(row.morning_in, exp["morning_in"])
        mo = show_time(row.morning_out, exp["morning_out"])
        ai = show_time(row.afternoon_in, exp["afternoon_in"])
        ao = show_time(row.afternoon_out, exp["afternoon_out"])

        mm = diff_minutes(mi, mo)
        ma = diff_minutes(ai, ao)

        cons_rows = AttendanceConsumption.query.filter_by(attendance_id=row.id).order_by(AttendanceConsumption.idx.asc()).all()

        calc = compute_work_minutes_and_flags(row)
        payable_time = fmt_minutes(calc["payable_min"])
        warn_text = " | ".join(calc["warnings"]) if calc["warnings"] else ""

        chip = ""
        chip_class = "chip-rp"
        if vac:
            chip = "VAC"
            chip_class = "chip-vac"
        elif row.novelty == "Razones particulares":
            chip = "RP"
            chip_class = "chip-rp"
        elif row.novelty == "Curso":
            chip = "Curso"
            chip_class = "chip-curso"
        elif row.novelty == "Delivery":
            chip = "DEL"
            chip_class = "chip-curso"

        novelty_ui = (row.novelty or "")
        if vac and novelty_ui != "Vacaciones":
            novelty_ui = "Vacaciones"

        rows.append({
            "employee": emp_name,
            "key": key,
            "group": g,
            "mode": (row.mode or "AUTO"),
            "morning_in": mi,
            "morning_out": mo,
            "afternoon_in": ai,
            "afternoon_out": ao,
            "morning_time": fmt_minutes(mm),
            "afternoon_time": fmt_minutes(ma),
            "payable_time": payable_time,
            "novelty": novelty_ui,
            "novelty_minutes": int(row.novelty_minutes or 0),
            "notes": row.notes or "",
            "cons_count": len(cons_rows),
            "cons": cons_rows,
            "is_vac": bool(vac),
            "lock_times": bool(vac or (row.novelty == "Delivery")),
            "has_warning": bool(calc["warnings"]),
            "warn_text": warn_text,
            "chip": chip,
            "chip_class": chip_class,
            "mi_preset": mi_preset,
            "mo_preset": mo_preset,
            "ai_preset": ai_preset,
            "ao_preset": ao_preset,
        })

    if auto_fix_commit:
        db.session.commit()
    summary = attendance_summary_rows(start_date, end_date, empf, novf)

    # Totales del periodo filtrado
    sum_payable_min = 0
    sum_cons = 0
    for rr in summary:
        # rr['payable'] es HH:MM
        try:
            hh, mm = str(rr.get('payable','0:0')).split(':')
            sum_payable_min += int(hh)*60 + int(mm)
        except:
            pass
        sum_cons += int(rr.get('cons_total') or 0)
    sum_hours_txt = fmt_minutes(sum_payable_min)

    return render_template_string(
        ASISTENCIA_HTML,
        base_css=BASE_CSS,
        d=d,
        hday=hday,
        ga=GROUP_A, gb=GROUP_B,
        rows=rows,
        novelty_items=NOVELTY_ITEMS,
        maxc=MAX_CONSUMOS,
        err=err,
        msg=msg,
        employees=EMPLOYEES,
        emp=empf,
        start=start,
        end=end,
        novf=novf,
        summary=summary,
        sum_hours_txt=sum_hours_txt,
        sum_cons_money=sum_cons,
    )
@app.route("/asistencia/export/excel")
@login_required
def att_export_excel():
    emp = request.args.get("emp") or "ALL"
    novf = request.args.get("novf") or "ALL"
    start_date, end_date = parse_range_params(request.args.get("start"), request.args.get("end"))
    data = attendance_summary_rows(start_date, end_date, emp, novf)

    wb = Workbook()
    ws = wb.active
    ws.title = "Asistencia"
    ws.append(["Fecha","Empleado","Grupo","Mañana","Tarde","Horas pagas","Novedad","Notas","Consumos total","Consumos items"])

    for r in data:
        ws.append([
            r["day"], r["employee"], r["group"], r["morning"], r["afternoon"],
            r["payable"], r["novelty"], r["notes"],
            r["cons_total"], r["cons_items"]
        ])

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    filename = f"asistencia_{start_date.isoformat()}_{end_date.isoformat()}.xlsx"
    return send_file(bio, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.route("/asistencia/export/pdf")
@login_required
def att_export_pdf():
    emp = request.args.get("emp") or "ALL"
    novf = request.args.get("novf") or "ALL"
    start_date, end_date = parse_range_params(request.args.get("start"), request.args.get("end"))
    data = attendance_summary_rows(start_date, end_date, emp, novf)

    bio = BytesIO()
    c = canvas.Canvas(bio, pagesize=landscape(A4))
    width, height = landscape(A4)

    title = f"Asistencia ({'Todos' if emp=='ALL' else emp}) {start_date.isoformat()} a {end_date.isoformat()}"
    if novf != "ALL":
        title += f" | {novf}"

    c.setFont("Helvetica-Bold", 14)
    c.drawString(30, height - 30, title)

    headers = ["Fecha","Empleado","Grupo","Mañana","Tarde","Pagas","Novedad","Consumos","Notas"]
    colw = [70, 90, 45, 95, 95, 55, 110, 80, 260]
    x0 = 30
    y = height - 55

    def draw_row(vals, y_):
        x = x0
        for v, w in zip(vals, colw):
            s = str(v) if v is not None else ""
            if len(s) > 55 and w >= 200:
                s = s[:55] + "..."
            c.drawString(x, y_, s)
            x += w

    c.setFont("Helvetica-Bold", 9)
    draw_row(headers, y)
    c.line(30, y - 3, width - 30, y - 3)
    y -= 16

    c.setFont("Helvetica", 9)
    for r in data:
        if y < 40:
            c.showPage()
            c.setFont("Helvetica-Bold", 14)
            c.drawString(30, height - 30, title + " (cont.)")
            y = height - 55
            c.setFont("Helvetica-Bold", 9)
            draw_row(headers, y)
            c.line(30, y - 3, width - 30, y - 3)
            y -= 16
            c.setFont("Helvetica", 9)

        cons_txt = f"{r['cons_total']}"
        draw_row(
            [r["day"], r["employee"], r["group"], r["morning"], r["afternoon"], r["payable"], r["novelty"], cons_txt, r["notes"]],
            y
        )
        y -= 14

    c.save()
    bio.seek(0)
    filename = f"asistencia_{start_date.isoformat()}_{end_date.isoformat()}.pdf"
    return send_file(bio, as_attachment=True, download_name=filename, mimetype="application/pdf")

# ==============================
# IMPORT ASISTENCIA
# ==============================
IMPORT_ATT_HTML = """
<!doctype html>
<html lang="es">
<head><meta charset="utf-8"><title>Import Asistencia</title>{{ base_css|safe }}</head>
<body style="max-width:950px;">
  <a href="{{ url_for('asistencia') }}">← Volver</a>
  <h2>Importar datos (Excel) - Asistencia</h2>

  <div class="box" style="margin-bottom:12px;">
    <b>Formato:</b> hoja <code>attendance</code> (y opcional <code>consumptions</code>).<br>
    Columnas mínimas: <code>day</code>, <code>employee</code>, <code>group</code>, <code>morning_in</code>, <code>morning_out</code>,
    <code>afternoon_in</code>, <code>afternoon_out</code>, <code>novelty</code>, <code>novelty_minutes</code>, <code>notes</code>.
  </div>

  {% if msg %}<div class="box" style="border-color:#7ad2a4;background:#dff5e6;"><b>{{msg}}</b></div>{% endif %}
  {% if err %}<div class="box" style="border-color:#ffb866;background:#ffe6d6;"><b>Error:</b> {{err}}</div>{% endif %}

  <form method="post" enctype="multipart/form-data" style="margin-top:12px;">
    <div class="box">
      <label><b>Archivo Excel</b></label><br>
      <input type="file" name="file" accept=".xlsx" required>
      <br><br>
      <label><b>Modo</b></label><br>
      <select name="mode">
        <option value="skip" selected>Si existe el día/empleado, NO tocar (skip)</option>
        <option value="replace">Si existe el día/empleado, REEMPLAZAR (replace)</option>
      </select>
    </div>
    <br>
    <button class="btn">Importar</button>
  </form>
</body>
</html>
"""

@app.route("/import/asistencia", methods=["GET","POST"])
@login_required
def import_asistencia():
    err = None
    msg = None
    if request.method == "POST":
        mode = (request.form.get("mode") or "skip").strip()
        f = request.files.get("file")
        if not f:
            err = "No se recibió archivo."
        else:
            try:
                wb = load_workbook(f, data_only=True)
                if "attendance" not in wb.sheetnames:
                    raise ValueError("No existe la hoja 'attendance'.")

                ws = wb["attendance"]
                rows = list(ws.iter_rows(values_only=True))
                if len(rows) < 2:
                    raise ValueError("La hoja 'attendance' no tiene datos.")

                h = [str(x).strip() if x is not None else "" for x in rows[0]]
                need = ["day","employee","group","morning_in","morning_out","afternoon_in","afternoon_out","novelty","novelty_minutes","notes"]
                for n in need:
                    if n not in h:
                        raise ValueError(f"Falta columna '{n}' en attendance.")
                idx = {k:i for i,k in enumerate(h)}

                imported = 0
                replaced = 0

                for r in rows[1:]:
                    if not r or all(v is None or str(v).strip()=="" for v in r):
                        continue

                    day_v = r[idx["day"]]
                    if isinstance(day_v, datetime):
                        day_v = day_v.date()
                    elif isinstance(day_v, date):
                        pass
                    else:
                        day_v = date.fromisoformat(str(day_v).strip()[:10])

                    emp = str(r[idx["employee"]]).strip()
                    if emp not in EMPLOYEES:
                        continue

                    existing = Attendance.query.filter_by(day=day_v, employee=emp).first()
                    if existing:
                        if mode == "skip":
                            continue
                        AttendanceConsumption.query.filter_by(attendance_id=existing.id).delete()
                        db.session.delete(existing)
                        db.session.flush()
                        replaced += 1

                    grp = (str(r[idx["group"]]).strip().upper() or "A")
                    if grp not in ("A","B"):
                        grp = "A"

                    mi = (str(r[idx["morning_in"]]).strip() if r[idx["morning_in"]] else None)
                    mo = (str(r[idx["morning_out"]]).strip() if r[idx["morning_out"]] else None)
                    ai = (str(r[idx["afternoon_in"]]).strip() if r[idx["afternoon_in"]] else None)
                    ao = (str(r[idx["afternoon_out"]]).strip() if r[idx["afternoon_out"]] else None)

                    nov = (str(r[idx["novelty"]]).strip() if r[idx["novelty"]] else "") or ""
                    if nov not in NOVELTY_ITEMS:
                        nov = ""

                    novm = int(r[idx["novelty_minutes"]] or 0)

                    raw_notes_cell = r[idx["notes"]]  # no uses "if cell"
                    raw_notes = None if raw_notes_cell is None else str(raw_notes_cell).strip()

                    cons, notes_clean = extract_consumptions_and_clean_notes(
                        raw_notes, max_items=MAX_CONSUMOS
                    )

                    a = Attendance(
                        day=day_v,
                        employee=emp,
                        group_code=grp,
                        mode="MANUAL",
                        morning_in=mi,
                        morning_out=mo,
                        afternoon_in=ai,
                        afternoon_out=ao,
                        novelty=nov or None,
                        novelty_minutes=novm,
                        notes=notes_clean
                    )
                    db.session.add(a)
                    db.session.flush()

                    # Crear consumos detectados
                    for i, (item, amount) in enumerate(cons, start=1):
                        db.session.add(
                            AttendanceConsumption(
                                attendance_id=a.id,
                                idx=i,
                                item=item,
                                amount=amount
                            )
                        )

                    imported += 1

                db.session.commit()
                msg = f"Import OK. Registros: {imported}. Reemplazados: {replaced}."
            except Exception as ex:
                db.session.rollback()
                err = str(ex)

    return render_template_string(IMPORT_ATT_HTML, base_css=BASE_CSS, err=err, msg=msg)

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    # En local (python app.py) sí usamos app.run.
    # En Render esto NO corre (porque Render usa gunicorn).
    port = int(os.getenv("PORT", "5050"))
    app.run(host="127.0.0.1", port=port, debug=False)
