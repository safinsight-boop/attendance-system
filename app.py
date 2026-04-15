#!/usr/bin/env python3
"""
نظام إدارة الحضور والغياب — TTLock Integration
Flask + SQLite + APScheduler + Gmail SMTP
"""
import os, sqlite3, hashlib, requests, smtplib, logging, io, secrets
from datetime import datetime, timedelta, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify, render_template, send_file, session
from functools import wraps
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-change-in-prod')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════
DB_PATH    = 'attendance.db'
TTBASE     = 'https://euapi.ttlock.com'
CID        = os.getenv('TTLOCK_CLIENT_ID', '')
CSECRET    = os.getenv('TTLOCK_CLIENT_SECRET', '')
TTUSR      = os.getenv('TTLOCK_USERNAME', '')
TTPASS     = os.getenv('TTLOCK_PASSWORD', '')
EMAIL_FROM       = os.getenv('EMAIL_SENDER', '')
EMAIL_PASS       = os.getenv('EMAIL_PASSWORD', '')
SITE_URL         = os.getenv('SITE_URL', 'https://attendance-system-pd27.onrender.com')
GRACE_MIN        = 5    # دقائق السماح قبل احتساب التأخر
AUTO_REJECT_DAYS = 3    # أيام الرفض التلقائي للعذر

# جدول المخالفات: {bracket: [(ptype, pvalue), ...]}
# ptype: 'warning' | 'percent' | 'day' | 'warning_day'
# percent → % من الأجر اليومي | day → N × الأجر اليومي
PENALTIES = {
    'late_1_15':   [('warning', 0),   ('percent', 5),  ('percent', 10), ('percent', 20)],
    'late_15_30':  [('percent', 10),  ('percent', 15), ('percent', 25), ('percent', 50)],
    'late_30_60':  [('percent', 25),  ('percent', 50), ('percent', 75), ('day', 1)],
    'late_60plus': [('warning_day', 1),('day', 2),      ('day', 3),      ('day', 3)],
    'early_u15':   [('warning', 0),   ('percent', 10), ('percent', 25), ('day', 1)],
    'early_o15':   [('percent', 10),  ('percent', 25), ('percent', 50), ('day', 1)],
    'flex_hours':  [('hours', 0)],   # خاص بالموظفين المرنين
    'absent_1':    [('percent', 50), ('day', 1), ('day', 2), ('day', 3)],
}

MONTHS_AR = ['يناير','فبراير','مارس','أبريل','مايو','يونيو',
             'يوليو','أغسطس','سبتمبر','أكتوبر','نوفمبر','ديسمبر']

_tt_cache = {'token': None, 'exp': 0}

# ═══════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS employees (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name_ar      TEXT NOT NULL,
            name_en      TEXT NOT NULL UNIQUE,
            email        TEXT,
            salary       REAL DEFAULT 0,
            housing      REAL DEFAULT 0,
            transport    REAL DEFAULT 0,
            commission   REAL DEFAULT 0,
            other_ded    REAL DEFAULT 0,
            work_type    TEXT DEFAULT 'fixed',
            work_start   TEXT DEFAULT '08:00',
            work_end     TEXT DEFAULT '17:00',
            weekly_hours REAL DEFAULT 40,
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS attendance (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            att_date    TEXT NOT NULL,
            check_in    TEXT,
            check_out   TEXT,
            late_min    INTEGER DEFAULT 0,
            early_min   INTEGER DEFAULT 0,
            total_hours REAL DEFAULT 0,
            status      TEXT DEFAULT 'present',
            FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE,
            UNIQUE(employee_id, att_date)
        );

        CREATE TABLE IF NOT EXISTS violations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            vio_date    TEXT NOT NULL,
            vtype       TEXT NOT NULL,
            bracket     TEXT NOT NULL,
            occurrence  INTEGER NOT NULL,
            ptype       TEXT NOT NULL,
            pvalue      REAL DEFAULT 0,
            deduction   REAL DEFAULT 0,
            FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS vio_counts (
            employee_id INTEGER NOT NULL,
            yr          INTEGER NOT NULL,
            mo          INTEGER NOT NULL,
            bracket     TEXT NOT NULL,
            cnt         INTEGER DEFAULT 0,
            PRIMARY KEY (employee_id, yr, mo, bracket)
        );

        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role          TEXT NOT NULL DEFAULT 'employee',
            employee_id   INTEGER,
            reset_token   TEXT,
            reset_exp     TEXT,
            created_at    TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS excuse_requests (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id  INTEGER NOT NULL,
            att_date     TEXT NOT NULL,
            vtype        TEXT NOT NULL,
            reason       TEXT NOT NULL,
            submitted_at TEXT DEFAULT (datetime('now')),
            status       TEXT DEFAULT 'pending',
            decided_by   INTEGER,
            decided_at   TEXT,
            manager_note TEXT,
            FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS leaves (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id  INTEGER NOT NULL,
            leave_type   TEXT NOT NULL,
            start_date   TEXT NOT NULL,
            end_date     TEXT NOT NULL,
            days         INTEGER NOT NULL,
            status       TEXT DEFAULT 'pending',
            approved_by  INTEGER,
            sick_doc     TEXT,
            notes        TEXT,
            created_at   TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS public_holidays (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            h_date     TEXT UNIQUE NOT NULL,
            name       TEXT NOT NULL,
            created_by INTEGER,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS overtime_requests (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id    INTEGER NOT NULL,
            att_date       TEXT NOT NULL,
            overtime_hours REAL NOT NULL,
            check_out_time TEXT NOT NULL,
            work_end       TEXT NOT NULL,
            status         TEXT DEFAULT 'pending',
            decided_by     INTEGER,
            decided_at     TEXT,
            FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS attendance_requests (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id     INTEGER NOT NULL,
            req_date        TEXT NOT NULL,
            req_type        TEXT NOT NULL,
            reason          TEXT NOT NULL,
            requested_time  TEXT,
            attachment      TEXT,
            attachment_name TEXT,
            submitted_at    TEXT DEFAULT (datetime('now')),
            status          TEXT DEFAULT 'pending',
            decided_by      INTEGER,
            decided_at      TEXT,
            manager_note    TEXT,
            FOREIGN KEY (employee_id) REFERENCES employees(id) ON DELETE CASCADE
        );
        """)
        conn.commit()
        _migrate_db(conn)
        _seed_default_user(conn)
        logger.info("Database initialized OK")
    finally:
        conn.close()

def _migrate_db(conn):
    """إضافة أعمدة جديدة لجداول موجودة (آمن عند التكرار)"""
    migrations = [
        "ALTER TABLE excuse_requests ADD COLUMN attachment TEXT",
        "ALTER TABLE excuse_requests ADD COLUMN attachment_name TEXT",
        "ALTER TABLE leaves ADD COLUMN attachment TEXT",
        "ALTER TABLE leaves ADD COLUMN attachment_name TEXT",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except Exception:
            pass
    conn.commit()

def _seed_default_user(conn):
    count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        pw = hashlib.sha256('admin123'.encode()).hexdigest()
        conn.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?,?,?)",
            ('admin', pw, 'hr'))
        conn.commit()
        logger.info("Default HR user created — username: admin / password: admin123")

# ═══════════════════════════════════════════════════════════
#  AUTH HELPERS
# ═══════════════════════════════════════════════════════════
def _hash(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'error': 'غير مصرح', 'login_required': True}), 401
        return f(*args, **kwargs)
    return decorated

def hr_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'error': 'غير مصرح', 'login_required': True}), 401
        if session.get('role') not in ('hr', 'manager'):
            return jsonify({'error': 'صلاحيات غير كافية'}), 403
        return f(*args, **kwargs)
    return decorated

def _gosi(emp):
    """احتساب تأمينات GOSI = 10.75% من الأجور الثابتة (راتب + سكن + نقل)"""
    return round((emp['salary'] + emp['housing'] + emp['transport']) * 0.1075, 2)

def _is_on_leave(conn, emp_id, target_date):
    """هل الموظف في إجازة معتمدة في هذا اليوم؟"""
    ds = str(target_date)
    # إجازة رسمية
    if conn.execute("SELECT 1 FROM public_holidays WHERE h_date=?", (ds,)).fetchone():
        return 'official_holiday'
    # إجازة شخصية معتمدة
    row = conn.execute("""
        SELECT leave_type FROM leaves
        WHERE employee_id=? AND status='approved'
        AND start_date<=? AND end_date>=?
    """, (emp_id, ds, ds)).fetchone()
    return row['leave_type'] if row else None

# ═══════════════════════════════════════════════════════════
#  TTLOCK API
# ═══════════════════════════════════════════════════════════
def _md5(s):
    return hashlib.md5(s.encode()).hexdigest()

def tt_get_token():
    global _tt_cache
    now = datetime.now().timestamp()
    if _tt_cache['token'] and now < _tt_cache['exp'] - 120:
        return _tt_cache['token']
    try:
        r = requests.post(f"{TTBASE}/oauth2/token", data={
            'client_id':     CID,
            'client_secret': CSECRET,
            'grant_type':    'password',
            'username':      TTUSR,
            'password':      _md5(TTPASS),
        }, timeout=15)
        d = r.json()
        if 'access_token' in d:
            _tt_cache = {'token': d['access_token'], 'exp': now + d.get('expires_in', 7200)}
            logger.info("TTLock token refreshed")
            return _tt_cache['token']
        logger.error(f"TTLock auth failed: {d}")
    except Exception as e:
        logger.error(f"TTLock auth error: {e}")
    return None

def tt_get_locks(token):
    locks, page = [], 1
    ts = int(datetime.now().timestamp() * 1000)
    while True:
        try:
            r = requests.get(f"{TTBASE}/v3/lock/list", params={
                'clientId': CID, 'accessToken': token,
                'pageNo': page, 'pageSize': 100, 'date': ts
            }, timeout=15).json()
            if r.get('errcode', -1) != 0:
                logger.warning(f"tt_get_locks err: {r}")
                break
            batch = r.get('list', [])
            locks.extend(batch)
            if len(batch) < 100: break
            page += 1
        except Exception as e:
            logger.error(f"tt_get_locks: {e}"); break
    return locks

def tt_get_records(token, lock_id, start_ms, end_ms):
    recs, page = [], 1
    ts = int(datetime.now().timestamp() * 1000)
    while True:
        try:
            r = requests.get(f"{TTBASE}/v3/lockRecord/list", params={
                'clientId': CID, 'accessToken': token,
                'lockId': lock_id, 'startDate': start_ms,
                'endDate': end_ms, 'pageNo': page,
                'pageSize': 100, 'date': ts
            }, timeout=15).json()
            if r.get('errcode', -1) != 0: break
            batch = r.get('list', [])
            recs.extend(batch)
            if len(batch) < 100: break
            page += 1
        except Exception as e:
            logger.error(f"tt_get_records lock={lock_id}: {e}"); break
    return recs

def fetch_daily_records(target_date):
    """
    جلب سجلات TTLock ليوم محدد.
    يرجع: {name_en_lower: [datetime, ...]} مرتبة زمنياً
    """
    token = tt_get_token()
    if not token:
        logger.error("No TTLock token — cannot fetch records")
        return {}

    start_ms = int(datetime(
        target_date.year, target_date.month, target_date.day, 0, 0, 0
    ).timestamp() * 1000)
    end_ms = int(datetime(
        target_date.year, target_date.month, target_date.day, 23, 59, 59
    ).timestamp() * 1000)

    by_user = {}
    for lock in tt_get_locks(token):
        lid = lock.get('lockId')
        if not lid: continue
        for rec in tt_get_records(token, lid, start_ms, end_ms):
            uname = (rec.get('username') or '').strip().lower()
            ts_ms = rec.get('successDate', 0)
            if uname and ts_ms:
                by_user.setdefault(uname, []).append(
                    datetime.fromtimestamp(ts_ms / 1000)
                )

    for k in by_user:
        by_user[k].sort()
    logger.info(f"TTLock: fetched records for {len(by_user)} users on {target_date}")
    return by_user

# ═══════════════════════════════════════════════════════════
#  VIOLATION ENGINE
# ═══════════════════════════════════════════════════════════
def late_bracket(mins):
    if mins <= 0:  return None
    if mins <= 15: return 'late_1_15'
    if mins <= 30: return 'late_15_30'
    if mins <= 60: return 'late_30_60'
    return 'late_60plus'

def early_bracket(mins):
    if mins <= 0:  return None
    if mins < 15:  return 'early_u15'
    return 'early_o15'

def next_occurrence(conn, emp_id, yr, mo, bracket):
    """زيادة عداد المخالفة وإرجاع الرقم الجديد"""
    row = conn.execute(
        "SELECT cnt FROM vio_counts WHERE employee_id=? AND yr=? AND mo=? AND bracket=?",
        (emp_id, yr, mo, bracket)
    ).fetchone()
    cnt = (row['cnt'] if row else 0) + 1
    conn.execute("""
        INSERT INTO vio_counts (employee_id, yr, mo, bracket, cnt)
        VALUES (?,?,?,?,?)
        ON CONFLICT(employee_id,yr,mo,bracket) DO UPDATE SET cnt=excluded.cnt
    """, (emp_id, yr, mo, bracket, cnt))
    return cnt

def calc_deduction(emp, ptype, pvalue):
    """حساب مبلغ الخصم النقدي"""
    daily = (emp['salary'] + emp['housing'] + emp['transport']) / 30
    if ptype == 'warning':               return 0.0
    if ptype == 'percent':               return round(daily * pvalue / 100, 2)
    if ptype in ('day', 'warning_day'):  return round(daily * pvalue, 2)
    return 0.0

def apply_violation(conn, emp, vio_date, vtype, bracket):
    """تطبيق المخالفة وحفظها، يرجع (ptype, pvalue, deduction)"""
    yr, mo = vio_date.year, vio_date.month
    occ = next_occurrence(conn, emp['id'], yr, mo, bracket)
    idx = min(occ, 4) - 1
    ptype, pvalue = PENALTIES[bracket][idx]
    ded = calc_deduction(emp, ptype, pvalue)
    conn.execute("""
        INSERT INTO violations
            (employee_id, vio_date, vtype, bracket, occurrence, ptype, pvalue, deduction)
        VALUES (?,?,?,?,?,?,?,?)
    """, (emp['id'], str(vio_date), vtype, bracket, occ, ptype, pvalue, ded))
    return ptype, pvalue, ded

# ═══════════════════════════════════════════════════════════
#  EMAIL NOTIFICATIONS
# ═══════════════════════════════════════════════════════════
def send_email(to, subject, html_body):
    if not EMAIL_FROM or not EMAIL_PASS or not to:
        return
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From']    = EMAIL_FROM
        msg['To']      = to
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_FROM, EMAIL_PASS)
            smtp.sendmail(EMAIL_FROM, to, msg.as_bytes())
        logger.info(f"Email sent → {to}: {subject}")
    except Exception as e:
        logger.error(f"Email error: {e}")

_STYLE = "font-family:Tahoma,Arial;direction:rtl;padding:28px;max-width:620px;margin:0 auto;color:#1e293b"
_TABLE = "border-collapse:collapse;width:100%;margin-top:14px"
_TD    = "padding:10px 14px;border:1px solid #e2e8f0;font-size:14px"

def notify_attendance(emp, att_date, status, check_in, check_out,
                      late_min, early_min, ptype=None, pvalue=None, ded=0.0):
    if not emp.get('email'): return
    name = emp.get('name_ar') or emp.get('name_en', '')
    ds   = str(att_date)
    ci   = check_in.strftime('%I:%M %p')  if check_in  else '—'
    co   = check_out.strftime('%I:%M %p') if check_out else '—'

    def penalty_text():
        if not ptype or ptype == 'warning': return 'إنذار كتابي'
        if ptype == 'percent': return f"خصم {pvalue}% من الأجر اليومي"
        if ptype in ('day', 'warning_day'): return f"خصم {pvalue} يوم من الراتب"
        return '—'

    if status == 'on_time':
        subj = f"✅ شكر وتقدير على الالتزام — {ds}"
        body = f"""<div style="{_STYLE}">
          <h2 style="color:#16a34a;margin-bottom:6px">✅ شكر وتقدير</h2>
          <p>عزيزي/عزيزتي <b>{name}</b>،</p>
          <p>نشكرك على التزامك بمواعيد العمل اليوم <b>{ds}</b>.</p>
          <table style="{_TABLE}">
            <tr><td style="{_TD};background:#f8fafc">وقت الحضور</td><td style="{_TD}"><b>{ci}</b></td></tr>
            <tr><td style="{_TD};background:#f8fafc">وقت الانصراف</td><td style="{_TD}"><b>{co}</b></td></tr>
          </table>
          <p style="color:#64748b;margin-top:16px;font-size:13px">نقدر جهدك وانضباطك.</p></div>"""

    elif status == 'late':
        subj = f"⚠️ إشعار تأخر — {ds}"
        ded_row = f'<tr><td style="{_TD};background:#fff1f2">مبلغ الخصم</td><td style="{_TD};color:#dc2626"><b>{ded:.2f} ر.س</b></td></tr>' if ded > 0 else ''
        body = f"""<div style="{_STYLE}">
          <h2 style="color:#d97706;margin-bottom:6px">⚠️ إشعار مخالفة — تأخر</h2>
          <p>عزيزي/عزيزتي <b>{name}</b>،</p>
          <p>تم رصد تأخر في حضورك بتاريخ <b>{ds}</b>.</p>
          <table style="{_TABLE}">
            <tr><td style="{_TD};background:#f8fafc">وقت الحضور</td><td style="{_TD}"><b>{ci}</b></td></tr>
            <tr><td style="{_TD};background:#fff7ed">مدة التأخر</td><td style="{_TD};color:#dc2626"><b>{late_min} دقيقة</b></td></tr>
            <tr><td style="{_TD};background:#f8fafc">العقوبة</td><td style="{_TD}"><b>{penalty_text()}</b></td></tr>
            {ded_row}
          </table>
          <p style="margin-top:16px">إذا كان لديك عذر يمكنك رفعه مباشرة:</p>
          <a href="{SITE_URL}" style="display:inline-block;background:#3b82f6;color:#fff;padding:10px 22px;border-radius:8px;text-decoration:none;font-weight:700;margin-top:6px">
            رفع العذر
          </a>
          <p style="color:#94a3b8;font-size:12px;margin-top:12px">سيتم رفض العذر تلقائياً إذا لم يُقدَّم خلال {AUTO_REJECT_DAYS} أيام.</p>
        </div>"""

    elif status == 'early_leave':
        subj = f"⚠️ إشعار مغادرة مبكرة — {ds}"
        ded_row = f'<tr><td style="{_TD};background:#fff1f2">مبلغ الخصم</td><td style="{_TD};color:#dc2626"><b>{ded:.2f} ر.س</b></td></tr>' if ded > 0 else ''
        body = f"""<div style="{_STYLE}">
          <h2 style="color:#d97706;margin-bottom:6px">⚠️ إشعار مخالفة — مغادرة مبكرة</h2>
          <p>عزيزي/عزيزتي <b>{name}</b>،</p>
          <p>تم رصد مغادرة مبكرة بتاريخ <b>{ds}</b>.</p>
          <table style="{_TABLE}">
            <tr><td style="{_TD};background:#f8fafc">وقت الانصراف</td><td style="{_TD}"><b>{co}</b></td></tr>
            <tr><td style="{_TD};background:#fff7ed">المغادرة المبكرة</td><td style="{_TD};color:#dc2626"><b>{early_min} دقيقة</b></td></tr>
            <tr><td style="{_TD};background:#f8fafc">العقوبة</td><td style="{_TD}"><b>{penalty_text()}</b></td></tr>
            {ded_row}
          </table>
          <p style="margin-top:16px">إذا كان لديك عذر يمكنك رفعه مباشرة:</p>
          <a href="{SITE_URL}" style="display:inline-block;background:#3b82f6;color:#fff;padding:10px 22px;border-radius:8px;text-decoration:none;font-weight:700;margin-top:6px">
            رفع العذر
          </a>
          <p style="color:#94a3b8;font-size:12px;margin-top:12px">سيتم رفض العذر تلقائياً إذا لم يُقدَّم خلال {AUTO_REJECT_DAYS} أيام.</p>
        </div>"""

    elif status == 'absent':
        subj = f"🔴 إشعار غياب — {ds}"
        ded_row = f'<tr><td style="{_TD};background:#fff1f2">مبلغ الخصم</td><td style="{_TD};color:#dc2626"><b>{ded:.2f} ر.س</b></td></tr>' if ded > 0 else ''
        body = f"""<div style="{_STYLE}">
          <h2 style="color:#dc2626;margin-bottom:6px">🔴 إشعار غياب</h2>
          <p>عزيزي/عزيزتي <b>{name}</b>،</p>
          <p>لم يتم تسجيل حضورك بتاريخ <b>{ds}</b>.</p>
          <table style="{_TABLE}">
            <tr><td style="{_TD};background:#f8fafc">العقوبة</td><td style="{_TD}"><b>{penalty_text()}</b></td></tr>
            {ded_row}
          </table>
          <p style="margin-top:16px">إذا كان لديك عذر يمكنك رفعه مباشرة:</p>
          <a href="{SITE_URL}" style="display:inline-block;background:#3b82f6;color:#fff;padding:10px 22px;border-radius:8px;text-decoration:none;font-weight:700;margin-top:6px">
            رفع العذر
          </a>
          <p style="color:#94a3b8;font-size:12px;margin-top:12px">سيتم رفض العذر تلقائياً إذا لم يُقدَّم خلال {AUTO_REJECT_DAYS} أيام.</p>
        </div>"""
    elif status.startswith('leave_'):
        return  # لا إشعار للإجازات المعتمدة
    else:
        return

    send_email(emp['email'], subj, body)

def _notify_overtime(emp, att_date, ot_hours, checkout_time, conn):
    """إشعار مدراء HR و القسم بوجود إضافي غير مؤكد"""
    managers = conn.execute(
        "SELECT u.*, e.email AS emp_email FROM users u "
        "LEFT JOIN employees e ON e.id=u.employee_id "
        "WHERE u.role IN ('hr','manager')"
    ).fetchall()
    name = emp.get('name_ar') or emp.get('name_en', '')
    for mgr in managers:
        to = mgr['emp_email'] or EMAIL_FROM
        if not to:
            continue
        subj = f"🕐 إضافي غير مؤكد — {name} — {att_date}"
        body = f"""<div style="{_STYLE}">
          <h2 style="color:#d97706;margin-bottom:6px">🕐 إشعار إضافي</h2>
          <p>الموظف <b>{name}</b> بقي <b>{ot_hours:.1f} ساعة</b> بعد انتهاء دوامه بتاريخ <b>{att_date}</b>.</p>
          <p>وقت الانصراف: <b>{checkout_time}</b></p>
          <p>هل يوجد <b>تكليف رسمي</b> لهذا الإضافي؟</p>
          <p>
            <a href="{SITE_URL}" style="background:#3b82f6;color:#fff;padding:10px 20px;border-radius:8px;text-decoration:none;font-weight:700">
              الرد من الموقع
            </a>
          </p>
          <p style="color:#94a3b8;font-size:12px;margin-top:14px">إذا لم يتم التأكيد خلال 24 ساعة لن يُحتسب الإضافي.</p>
        </div>"""
        send_email(to, subj, body)

def notify_flex_weekly(emp, friday, actual_h, required_h, ded):
    if not emp.get('email'): return
    name = emp.get('name_ar') or emp.get('name_en', '')
    monday = friday - timedelta(days=4)
    missing = max(0.0, required_h - actual_h)
    color = "#dc2626" if missing > 0 else "#16a34a"
    subj = f"📊 تقرير ساعاتك الأسبوعي — {monday} إلى {friday}"
    ded_row = f'<tr><td style="{_TD};background:#fff1f2;color:#dc2626">الخصم المقدر</td><td style="{_TD};color:#dc2626"><b>{ded:.2f} ر.س</b></td></tr>' if ded > 0 else ''
    body = f"""<div style="{_STYLE}">
      <h2 style="margin-bottom:6px">📊 تقرير الساعات الأسبوعي</h2>
      <p>عزيزي/عزيزتي <b>{name}</b>،</p>
      <p>أسبوع: <b>{monday}</b> — <b>{friday}</b></p>
      <table style="{_TABLE}">
        <tr><td style="{_TD};background:#f8fafc">الساعات المطلوبة</td><td style="{_TD}"><b>{required_h:.1f} ساعة</b></td></tr>
        <tr><td style="{_TD};background:#f8fafc">الساعات الفعلية</td><td style="{_TD}"><b>{actual_h:.1f} ساعة</b></td></tr>
        <tr><td style="{_TD};background:#f8fafc;color:{color}">الساعات الناقصة</td><td style="{_TD};color:{color}"><b>{missing:.1f} ساعة</b></td></tr>
        {ded_row}
      </table></div>"""
    send_email(emp['email'], subj, body)

# ═══════════════════════════════════════════════════════════
#  ATTENDANCE PROCESSING ENGINE
# ═══════════════════════════════════════════════════════════
def process_day(target_date=None):
    if target_date is None:
        target_date = date.today()

    logger.info(f"=== Processing attendance for {target_date} ===")
    raw = fetch_daily_records(target_date)

    conn = get_db()
    try:
        employees = conn.execute("SELECT * FROM employees").fetchall()

        for emp_row in employees:
            emp = dict(emp_row)
            uname = emp['name_en'].strip().lower()
            times = raw.get(uname, [])

            check_in  = times[0]  if times          else None
            check_out = times[-1] if len(times) > 1 else None

            late_min = early_min = 0
            total_hours = 0.0
            status = 'absent'
            ptype = pvalue = None
            ded = 0.0

            if not times:
                # فحص الإجازة قبل تسجيل غياب
                leave_type = _is_on_leave(conn, emp['id'], target_date)
                if leave_type:
                    status = f'leave_{leave_type}'
                else:
                    status = 'absent'
                    # مخالفة غياب يوم واحد
                    ptype, pvalue, ded = apply_violation(
                        conn, emp, target_date, 'absent', 'absent_1')

            elif emp['work_type'] == 'fixed':
                try:
                    wstart = datetime.strptime(
                        f"{target_date} {emp['work_start']}", "%Y-%m-%d %H:%M")
                    wend   = datetime.strptime(
                        f"{target_date} {emp['work_end']}",   "%Y-%m-%d %H:%M")
                except Exception:
                    wstart = wend = None

                if check_in and wstart:
                    raw_late = (check_in - wstart).total_seconds() / 60
                    # فترة السماح 5 دقائق
                    late_min = max(0, int(raw_late) - GRACE_MIN)

                if check_out and wend:
                    diff = (wend - check_out).total_seconds() / 60
                    early_min = max(0, int(diff))

                if check_in and check_out:
                    total_hours = round(
                        (check_out - check_in).total_seconds() / 3600, 2)

                # ── الأولوية: تأخر أولاً ثم مغادرة مبكرة ──
                if late_min > 0:
                    status = 'late'
                    br = late_bracket(late_min)
                    ptype, pvalue, ded = apply_violation(
                        conn, emp, target_date, 'late', br)
                    if early_min > 0:
                        apply_violation(conn, emp, target_date,
                                        'early_leave', early_bracket(early_min))
                elif early_min > 0:
                    status = 'early_leave'
                    br = early_bracket(early_min)
                    ptype, pvalue, ded = apply_violation(
                        conn, emp, target_date, 'early_leave', br)
                else:
                    status = 'on_time'

                # ── كشف الإضافي (بعد وقت الانصراف بأكثر من 30 دقيقة) ──
                if check_out and wend:
                    ot_min = (check_out - wend).total_seconds() / 60
                    if ot_min > 30:
                        ot_hours = round(ot_min / 60, 2)
                        conn.execute("""
                            INSERT OR IGNORE INTO overtime_requests
                                (employee_id, att_date, overtime_hours, check_out_time, work_end)
                            VALUES (?,?,?,?,?)
                        """, (emp['id'], str(target_date), ot_hours,
                              check_out.strftime('%H:%M'), emp['work_end']))
                        _notify_overtime(emp, target_date, ot_hours,
                                         check_out.strftime('%H:%M'), conn)

            else:  # flex
                if check_in and check_out:
                    total_hours = round(
                        (check_out - check_in).total_seconds() / 3600, 2)
                status = 'present'

            # حفظ سجل الحضور
            conn.execute("""
                INSERT INTO attendance
                    (employee_id, att_date, check_in, check_out,
                     late_min, early_min, total_hours, status)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(employee_id, att_date) DO UPDATE SET
                    check_in=excluded.check_in,
                    check_out=excluded.check_out,
                    late_min=excluded.late_min,
                    early_min=excluded.early_min,
                    total_hours=excluded.total_hours,
                    status=excluded.status
            """, (
                emp['id'], str(target_date),
                check_in.strftime('%H:%M')  if check_in  else None,
                check_out.strftime('%H:%M') if check_out else None,
                late_min, early_min, total_hours, status
            ))

            # إرسال إيميل الإشعار
            notify_attendance(emp, target_date, status, check_in, check_out,
                              late_min, early_min, ptype, pvalue, ded)

        # ── فحص الموظفين المرنين (كل جمعة) ──
        if target_date.weekday() == 4:
            monday = target_date - timedelta(days=4)
            week_dates = [str(monday + timedelta(days=i)) for i in range(5)]
            ph = ','.join(['?'] * 5)

            flex_emps = conn.execute(
                "SELECT * FROM employees WHERE work_type='flex'"
            ).fetchall()

            for emp_row in flex_emps:
                emp = dict(emp_row)
                row = conn.execute(
                    f"SELECT COALESCE(SUM(total_hours),0) AS h FROM attendance "
                    f"WHERE employee_id=? AND att_date IN ({ph})",
                    [emp['id']] + week_dates
                ).fetchone()

                actual_h   = row['h'] or 0.0
                required_h = emp['weekly_hours'] or 40.0
                missing    = max(0.0, required_h - actual_h)

                if missing > 0:
                    hourly = (emp['salary'] + emp['housing'] + emp['transport']
                              ) / (4 * required_h)
                    flex_ded = round(missing * hourly, 2)
                    occ = next_occurrence(
                        conn, emp['id'],
                        target_date.year, target_date.month, 'flex_hours')
                    conn.execute("""
                        INSERT INTO violations
                            (employee_id,vio_date,vtype,bracket,occurrence,ptype,pvalue,deduction)
                        VALUES (?,?,?,?,?,?,?,?)
                    """, (emp['id'], str(target_date), 'flex_hours',
                          'flex_hours', occ, 'hours', missing, flex_ded))
                    notify_flex_weekly(emp, target_date, actual_h, required_h, flex_ded)
                else:
                    notify_flex_weekly(emp, target_date, actual_h, required_h, 0.0)

        conn.commit()
        logger.info(f"=== Attendance processing done: {target_date} ===")

    except Exception as e:
        conn.rollback()
        logger.error(f"process_day error: {e}", exc_info=True)
        raise
    finally:
        conn.close()

# ═══════════════════════════════════════════════════════════
#  EXCEL — HELPERS
# ═══════════════════════════════════════════════════════════
def _bdr():
    s = Side(style='thin', color='BFBFBF')
    return Border(left=s, right=s, top=s, bottom=s)

def _cell(ws, row, col, val, bold=False, bg=None, fg='000000',
          size=11, wrap=False, num_fmt=None):
    c = ws.cell(row=row, column=col, value=val)
    c.font      = Font(bold=bold, size=size, color=fg,
                       name='Calibri')
    c.alignment = Alignment(horizontal='center', vertical='center',
                            wrap_text=wrap, reading_order=2)
    c.border    = _bdr()
    if bg:      c.fill = PatternFill('solid', fgColor=bg)
    if num_fmt: c.number_format = num_fmt
    return c

# ═══════════════════════════════════════════════════════════
#  EXCEL — ATTENDANCE EXPORT
# ═══════════════════════════════════════════════════════════
STATUS_AR = {
    'on_time': 'في الوقت', 'late': 'متأخر',
    'absent': 'غائب', 'early_leave': 'مغادرة مبكرة', 'present': 'حاضر'
}

def export_attendance_excel(year, month, emp_id=None):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "سجل الحضور"
    ws.sheet_view.rightToLeft = True
    ws.sheet_view.showGridLines = False

    col_widths = [22, 12, 10, 10, 10, 14, 16]
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.row_dimensions[1].height = 38
    ws.row_dimensions[2].height = 24

    # Title
    ws.merge_cells('A1:G1')
    _cell(ws, 1, 1,
          f"سجل الحضور والغياب — {MONTHS_AR[month-1]} {year}",
          bold=True, size=15, bg='1F4E79', fg='FFFFFF')

    # Headers
    hdrs = ['اسم الموظف', 'التاريخ', 'الحضور', 'الانصراف',
            'تأخر (دق)', 'خروج مبكر (دق)', 'الحالة']
    for i, h in enumerate(hdrs, 1):
        _cell(ws, 2, i, h, bold=True, bg='2E74B5', fg='FFFFFF')

    conn = get_db()
    try:
        q = """SELECT e.name_ar, a.*
               FROM attendance a
               JOIN employees e ON e.id = a.employee_id
               WHERE a.att_date LIKE ?"""
        params = [f"{year}-{month:02d}-%"]
        if emp_id:
            q += " AND a.employee_id=?"
            params.append(emp_id)
        q += " ORDER BY a.att_date, e.name_ar"
        rows = conn.execute(q, params).fetchall()
    finally:
        conn.close()

    BG = {'on_time': 'C6EFCE', 'late': 'FFC7CE',
          'absent': 'F2F2F2', 'early_leave': 'FFEB9C', 'present': 'DDEEFF'}

    for i, row in enumerate(rows, 3):
        s  = row['status']
        bg = BG.get(s)
        vals = [
            row['name_ar'], row['att_date'],
            row['check_in'] or '—', row['check_out'] or '—',
            row['late_min'] or 0, row['early_min'] or 0,
            STATUS_AR.get(s, s)
        ]
        for ci, v in enumerate(vals, 1):
            _cell(ws, i, ci, v, bg=bg)

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out

# ═══════════════════════════════════════════════════════════
#  EXCEL — PAYROLL EXPORT
# ═══════════════════════════════════════════════════════════
def export_payroll_excel(year, month):
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    C_HDR    = '1F4E79'
    C_SUB    = '2E74B5'
    C_GREEN  = 'C6EFCE'
    C_RED    = 'FFC7CE'
    C_YELLOW = 'FFEB9C'
    C_GRAY   = 'D9D9D9'
    C_ALT    = 'EBF3FB'

    conn = get_db()
    try:
        emps   = conn.execute("SELECT * FROM employees ORDER BY name_ar").fetchall()
        prefix = f"{year}-{month:02d}-%"
        payroll_summary = []

        for emp_row in emps:
            emp  = dict(emp_row)
            atts = conn.execute(
                "SELECT * FROM attendance WHERE employee_id=? AND att_date LIKE ? ORDER BY att_date",
                (emp['id'], prefix)).fetchall()
            vios = conn.execute(
                "SELECT * FROM violations WHERE employee_id=? AND vio_date LIKE ? ORDER BY vio_date",
                (emp['id'], prefix)).fetchall()

            total_ded = sum(v['deduction'] for v in vios)
            gross     = emp['salary'] + emp['housing'] + emp['transport'] + emp['commission']
            gosi_ded  = _gosi(emp)
            net       = gross - total_ded - gosi_ded

            # ── شيت الموظف ──
            ws = wb.create_sheet(emp['name_en'][:28])
            ws.sheet_view.rightToLeft = True
            ws.sheet_view.showGridLines = False
            for col, w in zip('ABCDEF', [14, 10, 10, 12, 32, 14]):
                ws.column_dimensions[col].width = w
            ws.row_dimensions[1].height = 38
            ws.row_dimensions[2].height = 24
            ws.row_dimensions[3].height = 24

            # Title
            ws.merge_cells('A1:F1')
            _cell(ws, 1, 1,
                  f"سجل حضور وغياب — {emp['name_ar']} — {MONTHS_AR[month-1]} {year}",
                  bold=True, size=14, bg=C_HDR, fg='FFFFFF')

            # Info row
            ws.merge_cells('A2:C2')
            ws.merge_cells('D2:F2')
            _cell(ws, 2, 1,
                  f"الراتب الأساسي: {emp['salary']:,.0f} ر.س  |  "
                  f"البدلات: {emp['housing']+emp['transport']:,.0f} ر.س",
                  bg=C_ALT)
            _cell(ws, 2, 4,
                  f"نوع الدوام: {'ثابت' if emp['work_type']=='fixed' else 'مرن'}  |  "
                  f"الدوام: {emp['work_start']} — {emp['work_end']}",
                  bg=C_ALT)

            # Column headers
            for ci, h in enumerate(
                ['التاريخ', 'الحضور', 'الانصراف', 'التأخر(دق)', 'المخالفة', 'الخصم(ر.س)'], 1
            ):
                _cell(ws, 3, ci, h, bold=True, bg=C_SUB, fg='FFFFFF')

            r = 4
            for att in atts:
                day_vios = [v for v in vios if v['vio_date'] == att['att_date']]
                vio_text = '; '.join([
                    f"{'تأخر' if v['vtype']=='late' else ('ساعات مرنة' if v['vtype']=='flex_hours' else 'مغادرة مبكرة')}"
                    f" ({v['ptype']} {v['pvalue']}{'%' if v['ptype']=='percent' else ' يوم' if v['ptype'] in ('day','warning_day') else ' س'})"
                    for v in day_vios
                ]) or '—'
                day_ded = sum(v['deduction'] for v in day_vios)

                s  = att['status']
                bg = ('C6EFCE' if s == 'on_time'
                      else 'FFC7CE' if s in ('late', 'absent')
                      else 'FFEB9C' if s == 'early_leave'
                      else None)

                vals = [
                    att['att_date'],
                    att['check_in']  or '—',
                    att['check_out'] or '—',
                    att['late_min']  or '—',
                    vio_text,
                    round(day_ded, 2) if day_ded else '—'
                ]
                for ci, v in enumerate(vals, 1):
                    c = _cell(ws, r, ci, v, bg=bg, wrap=(ci == 5))
                    if ci == 6 and isinstance(v, float):
                        c.number_format = '#,##0.00'
                r += 1

            # Totals block
            r += 1
            ws.merge_cells(f'A{r}:D{r}')
            _cell(ws, r, 1, 'إجمالي الخصومات:', bold=True, bg=C_GRAY)
            ws.merge_cells(f'E{r}:F{r}')
            _cell(ws, r, 5, total_ded, bold=True,
                  bg=C_RED if total_ded > 0 else C_GREEN,
                  num_fmt='#,##0.00 "ر.س"')

            r += 1
            ws.merge_cells(f'A{r}:D{r}')
            _cell(ws, r, 1, 'الراتب الإجمالي:', bold=True, bg=C_GRAY)
            ws.merge_cells(f'E{r}:F{r}')
            _cell(ws, r, 5, gross, bold=True, num_fmt='#,##0.00 "ر.س"')

            r += 1
            ws.merge_cells(f'A{r}:D{r}')
            _cell(ws, r, 1, 'صافي الراتب:', bold=True, size=12, bg=C_GRAY)
            ws.merge_cells(f'E{r}:F{r}')
            _cell(ws, r, 5, net, bold=True, size=13,
                  bg=C_GREEN if net >= gross * 0.9 else C_RED,
                  num_fmt='#,##0.00 "ر.س"')

            # Signature area
            r += 3
            for ci, lbl in [(1, 'توقيع الموظف:'), (3, 'توقيع المدير:'), (5, 'اعتماد الإدارة:')]:
                ws.cell(row=r, column=ci, value=lbl).font = Font(bold=True)
                ws.cell(row=r+2, column=ci, value='_' * 22)

            payroll_summary.append({
                **emp,
                'total_ded': total_ded,
                'gosi_ded':  gosi_ded,
                'other_ded': gosi_ded,
                'gross':     gross,
                'net':       net,
                'days':      len(atts),
                'present':   sum(1 for a in atts if a['status'] != 'absent'),
                'late_days': sum(1 for a in atts if a['status'] == 'late'),
            })

        # ── شيت مسيرة الرواتب (الشيت الأول) ──
        ws2 = wb.create_sheet("مسيرة الرواتب", 0)
        ws2.sheet_view.rightToLeft = True
        ws2.sheet_view.showGridLines = False

        col_ws = [5, 22, 14, 12, 14, 12, 14, 14, 16, 14]
        for i, w in enumerate(col_ws, 1):
            ws2.column_dimensions[get_column_letter(i)].width = w
        ws2.row_dimensions[1].height = 44
        ws2.row_dimensions[2].height = 26

        ws2.merge_cells('A1:J1')
        _cell(ws2, 1, 1,
              f"مسيرة الرواتب — {MONTHS_AR[month-1]} {year}",
              bold=True, size=17, bg=C_HDR, fg='FFFFFF')

        pay_hdrs = ['م', 'اسم الموظف', 'الراتب الأساسي', 'بدل سكن',
                    'بدل مواصلات', 'عمولة', 'خصومات', 'تأمينات (10.75%)',
                    'صافي الراتب', 'ملاحظات']
        for i, h in enumerate(pay_hdrs, 1):
            _cell(ws2, 2, i, h, bold=True, bg=C_SUB, fg='FFFFFF')

        tgross = tnet = tded = 0.0
        for idx, pd in enumerate(payroll_summary, 1):
            r   = idx + 2
            bg  = C_ALT if idx % 2 == 0 else None
            vals = [
                idx, pd['name_ar'],
                pd['salary'], pd['housing'], pd['transport'],
                pd['commission'], pd['total_ded'], pd['other_ded'],
                pd['net'], ''
            ]
            for ci, v in enumerate(vals, 1):
                c = _cell(ws2, r, ci, v, bg=bg)
                if ci in (3, 4, 5, 6, 7, 8, 9):
                    c.number_format = '#,##0.00'
            tgross += pd['gross']
            tnet   += pd['net']
            tded   += pd['total_ded'] + pd['other_ded']

        # Totals row
        tot = len(payroll_summary) + 3
        ws2.merge_cells(f'A{tot}:B{tot}')
        _cell(ws2, tot, 1, 'الإجمالي', bold=True, size=12, bg=C_GRAY)
        for ci in range(3, 11):
            _cell(ws2, tot, ci, '', bg=C_GRAY)
        _cell(ws2, tot, 7, tded, bold=True, bg=C_GRAY, num_fmt='#,##0.00')
        _cell(ws2, tot, 9, tnet, bold=True, size=13,
              bg=C_GREEN if tnet > 0 else C_RED, num_fmt='#,##0.00')

        # Signature
        sig = tot + 3
        for ci, lbl in [(1, 'إعداد:'), (4, 'مراجعة:'), (7, 'اعتماد:')]:
            ws2.cell(sig, ci, lbl).font = Font(bold=True)
            ws2.cell(sig + 2, ci, '_' * 26)

    finally:
        conn.close()

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out

# ═══════════════════════════════════════════════════════════
#  FLASK ROUTES
# ═══════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')

# ── Dashboard ────────────────────────────────────────────
@app.route('/api/stats/today')
def api_stats_today():
    today = str(date.today())
    conn = get_db()
    try:
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM employees").fetchone()['c']
        row = conn.execute("""
            SELECT
                SUM(CASE WHEN status='on_time'    THEN 1 ELSE 0 END) AS on_time,
                SUM(CASE WHEN status='late'        THEN 1 ELSE 0 END) AS late,
                SUM(CASE WHEN status='absent'      THEN 1 ELSE 0 END) AS absent,
                SUM(CASE WHEN status='early_leave' THEN 1 ELSE 0 END) AS early_leave,
                SUM(CASE WHEN status='present'     THEN 1 ELSE 0 END) AS present
            FROM attendance WHERE att_date=?""", (today,)).fetchone()
    finally:
        conn.close()
    return jsonify({
        'date': today, 'total_employees': total,
        'on_time':     row['on_time']     or 0,
        'late':        row['late']        or 0,
        'absent':      row['absent']      or 0,
        'early_leave': row['early_leave'] or 0,
        'present':     row['present']     or 0,
    })

@app.route('/api/stats/month')
def api_stats_month():
    y = request.args.get('year',  date.today().year,  type=int)
    m = request.args.get('month', date.today().month, type=int)
    prefix = f"{y}-{m:02d}-%"
    conn = get_db()
    try:
        att = conn.execute("""
            SELECT COUNT(*) AS total,
                SUM(CASE WHEN status='late'        THEN 1 ELSE 0 END) AS late,
                SUM(CASE WHEN status='absent'      THEN 1 ELSE 0 END) AS absent,
                SUM(CASE WHEN status='early_leave' THEN 1 ELSE 0 END) AS early_leave,
                SUM(COALESCE(late_min,0)) AS late_min_total
            FROM attendance WHERE att_date LIKE ?""", (prefix,)).fetchone()
        vio = conn.execute("""
            SELECT COALESCE(SUM(deduction),0) AS d, COUNT(*) AS c
            FROM violations WHERE vio_date LIKE ?""", (prefix,)).fetchone()
    finally:
        conn.close()
    return jsonify({
        'year': y, 'month': m,
        'total':          att['total']         or 0,
        'late':           att['late']           or 0,
        'absent':         att['absent']         or 0,
        'early_leave':    att['early_leave']    or 0,
        'late_min_total': att['late_min_total'] or 0,
        'violations':     vio['c']              or 0,
        'total_deductions': float(vio['d']      or 0),
    })

@app.route('/api/attendance/recent')
def api_attendance_recent():
    limit = request.args.get('limit', 15, type=int)
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT a.*, e.name_ar
            FROM attendance a
            JOIN employees e ON e.id = a.employee_id
            ORDER BY a.att_date DESC, a.check_in DESC
            LIMIT ?""", (limit,)).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/run', methods=['POST'])
def api_run():
    data = request.get_json(silent=True) or {}
    d_str = data.get('date')
    try:
        target = datetime.strptime(d_str, '%Y-%m-%d').date() if d_str else None
        process_day(target)
        return jsonify({'ok': True, 'msg': 'تمت معالجة الحضور بنجاح'})
    except Exception as e:
        logger.error(f"Manual run error: {e}", exc_info=True)
        return jsonify({'ok': False, 'msg': str(e)}), 500

# ── Attendance ────────────────────────────────────────────
@app.route('/api/attendance')
def api_attendance():
    y   = request.args.get('year',  date.today().year,  type=int)
    m   = request.args.get('month', date.today().month, type=int)
    eid = request.args.get('emp_id', type=int)
    prefix = f"{y}-{m:02d}-%"
    q = """SELECT a.*, e.name_ar
           FROM attendance a JOIN employees e ON e.id=a.employee_id
           WHERE a.att_date LIKE ?"""
    params = [prefix]
    if eid:
        q += " AND a.employee_id=?"
        params.append(eid)
    q += " ORDER BY a.att_date DESC, e.name_ar"
    conn = get_db()
    try:
        rows = conn.execute(q, params).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/attendance/export')
def api_attendance_export():
    y   = request.args.get('year',  date.today().year,  type=int)
    m   = request.args.get('month', date.today().month, type=int)
    eid = request.args.get('emp_id', type=int)
    out = export_attendance_excel(y, m, eid)
    return send_file(out,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f"attendance_{y}_{m:02d}.xlsx")

# ── Employees ─────────────────────────────────────────────
@app.route('/api/employees', methods=['GET'])
def api_emps_get():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM employees ORDER BY name_ar").fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/employees', methods=['POST'])
def api_emps_post():
    d = request.get_json(silent=True) or {}
    if not d.get('name_ar') or not d.get('name_en'):
        return jsonify({'error': 'name_ar و name_en مطلوبان'}), 400
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO employees
                (name_ar,name_en,email,salary,housing,transport,commission,
                 other_ded,work_type,work_start,work_end,weekly_hours)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (d['name_ar'], d['name_en'], d.get('email'),
             d.get('salary',0), d.get('housing',0), d.get('transport',0),
             d.get('commission',0), d.get('other_ded',0),
             d.get('work_type','fixed'), d.get('work_start','08:00'),
             d.get('work_end','17:00'), d.get('weekly_hours',40)))
        conn.commit()
        return jsonify({'ok': True, 'msg': 'تم إضافة الموظف بنجاح'})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'الاسم الإنجليزي مستخدم بالفعل'}), 400
    finally:
        conn.close()

@app.route('/api/employees/<int:eid>', methods=['GET'])
def api_emp_get(eid):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM employees WHERE id=?", (eid,)).fetchone()
    finally:
        conn.close()
    if not row:
        return jsonify({'error': 'الموظف غير موجود'}), 404
    return jsonify(dict(row))

@app.route('/api/employees/<int:eid>', methods=['PUT'])
def api_emp_put(eid):
    d = request.get_json(silent=True) or {}
    allowed = ['name_ar','name_en','email','salary','housing','transport',
               'commission','other_ded','work_type','work_start','work_end',
               'weekly_hours']
    updates = {k: d[k] for k in allowed if k in d}
    if not updates:
        return jsonify({'error': 'لا توجد حقول للتحديث'}), 400
    sql = f"UPDATE employees SET {', '.join(k+'=?' for k in updates)} WHERE id=?"
    conn = get_db()
    try:
        conn.execute(sql, list(updates.values()) + [eid])
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True, 'msg': 'تم تحديث بيانات الموظف'})

@app.route('/api/employees/<int:eid>', methods=['DELETE'])
def api_emp_delete(eid):
    conn = get_db()
    try:
        conn.execute("DELETE FROM employees WHERE id=?", (eid,))
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True, 'msg': 'تم حذف الموظف'})

# ── Payroll ───────────────────────────────────────────────
@app.route('/api/payroll')
def api_payroll():
    y = request.args.get('year',  date.today().year,  type=int)
    m = request.args.get('month', date.today().month, type=int)
    prefix = f"{y}-{m:02d}-%"
    conn = get_db()
    try:
        emps = conn.execute(
            "SELECT * FROM employees ORDER BY name_ar").fetchall()
        result = []
        for emp_row in emps:
            emp = dict(emp_row)
            vd = conn.execute(
                "SELECT COALESCE(SUM(deduction),0) AS d FROM violations "
                "WHERE employee_id=? AND vio_date LIKE ?",
                (emp['id'], prefix)).fetchone()
            ad = conn.execute("""
                SELECT COUNT(*) AS total,
                    SUM(CASE WHEN status='absent' THEN 1 ELSE 0 END) AS absent,
                    SUM(CASE WHEN status='late'   THEN 1 ELSE 0 END) AS late
                FROM attendance WHERE employee_id=? AND att_date LIKE ?""",
                (emp['id'], prefix)).fetchone()
            gross    = emp['salary'] + emp['housing'] + emp['transport'] + emp['commission']
            gosi_ded = _gosi(emp)
            net      = gross - (vd['d'] or 0) - gosi_ded
            result.append({
                **emp,
                'total_ded': round(vd['d'] or 0, 2),
                'gosi_ded':  gosi_ded,
                'other_ded': gosi_ded,
                'gross':     round(gross, 2),
                'net':       round(net, 2),
                'days':      ad['total']  or 0,
                'absent':    ad['absent'] or 0,
                'late':      ad['late']   or 0,
            })
    finally:
        conn.close()
    return jsonify(result)

@app.route('/api/payroll/export')
def api_payroll_export():
    y = request.args.get('year',  date.today().year,  type=int)
    m = request.args.get('month', date.today().month, type=int)
    out = export_payroll_excel(y, m)
    return send_file(out,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f"payroll_{y}_{m:02d}.xlsx")

@app.route('/api/violations')
def api_violations():
    y   = request.args.get('year',  date.today().year,  type=int)
    m   = request.args.get('month', date.today().month, type=int)
    eid = request.args.get('emp_id', type=int)
    q = """SELECT v.*, e.name_ar
           FROM violations v JOIN employees e ON e.id=v.employee_id
           WHERE v.vio_date LIKE ?"""
    params = [f"{y}-{m:02d}-%"]
    if eid:
        q += " AND v.employee_id=?"
        params.append(eid)
    conn = get_db()
    try:
        rows = conn.execute(q + " ORDER BY v.vio_date DESC", params).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

# ═══════════════════════════════════════════════════════════
#  AUTH ROUTES
# ═══════════════════════════════════════════════════════════
@app.route('/api/auth/login', methods=['POST'])
def api_login():
    d  = request.get_json(silent=True) or {}
    un = (d.get('username') or '').strip()
    pw = d.get('password', '')
    if not un or not pw:
        return jsonify({'error': 'اسم المستخدم وكلمة المرور مطلوبان'}), 400
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM users WHERE username=? AND password_hash=?",
            (un, _hash(pw))).fetchone()
        if not row:
            return jsonify({'error': 'بيانات الدخول غير صحيحة'}), 401
        session['user_id']   = row['id']
        session['username']  = row['username']
        session['role']      = row['role']
        session['employee_id'] = row['employee_id']
        return jsonify({'ok': True, 'role': row['role'], 'username': row['username'],
                        'employee_id': row['employee_id']})
    finally:
        conn.close()

@app.route('/api/auth/logout', methods=['POST'])
def api_logout():
    session.clear()
    return jsonify({'ok': True})

@app.route('/api/auth/me')
def api_me():
    if 'user_id' not in session:
        return jsonify({'logged_in': False})
    return jsonify({'logged_in': True, 'role': session.get('role'),
                    'username': session.get('username'),
                    'employee_id': session.get('employee_id')})

@app.route('/api/users', methods=['GET'])
@hr_required
def api_users_get():
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT u.id, u.username, u.role, u.created_at,
                   e.name_ar, e.name_en
            FROM users u LEFT JOIN employees e ON e.id=u.employee_id
            ORDER BY u.role, u.username
        """).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/users', methods=['POST'])
@hr_required
def api_users_post():
    d = request.get_json(silent=True) or {}
    un = (d.get('username') or '').strip()
    pw = d.get('password', '').strip()
    role = d.get('role', 'employee')
    emp_id = d.get('employee_id') or None
    if not un or not pw:
        return jsonify({'error': 'اسم المستخدم وكلمة المرور مطلوبان'}), 400
    if role not in ('hr', 'manager', 'employee'):
        return jsonify({'error': 'دور غير صحيح'}), 400
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO users (username, password_hash, role, employee_id) VALUES (?,?,?,?)",
            (un, _hash(pw), role, emp_id))
        conn.commit()
        return jsonify({'ok': True, 'msg': 'تم إنشاء المستخدم'})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'اسم المستخدم مستخدم بالفعل'}), 400
    finally:
        conn.close()

@app.route('/api/users/<int:uid>', methods=['DELETE'])
@hr_required
def api_user_delete(uid):
    if uid == session.get('user_id'):
        return jsonify({'error': 'لا يمكنك حذف حسابك الحالي'}), 400
    conn = get_db()
    try:
        conn.execute("DELETE FROM users WHERE id=?", (uid,))
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True})

@app.route('/api/users/<int:uid>/password', methods=['PUT'])
@hr_required
def api_user_password(uid):
    d = request.get_json(silent=True) or {}
    pw = d.get('password', '').strip()
    if not pw or len(pw) < 4:
        return jsonify({'error': 'كلمة المرور يجب أن تكون 4 أحرف على الأقل'}), 400
    conn = get_db()
    try:
        conn.execute("UPDATE users SET password_hash=? WHERE id=?", (_hash(pw), uid))
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True})

# ═══════════════════════════════════════════════════════════
#  EXCUSE ROUTES
# ═══════════════════════════════════════════════════════════
@app.route('/api/excuses', methods=['GET'])
@login_required
def api_excuses_get():
    conn = get_db()
    try:
        role = session.get('role')
        emp_id = session.get('employee_id')
        if role in ('hr', 'manager'):
            rows = conn.execute("""
                SELECT ex.*, e.name_ar, e.name_en
                FROM excuse_requests ex
                JOIN employees e ON e.id=ex.employee_id
                ORDER BY ex.submitted_at DESC
            """).fetchall()
        else:
            if not emp_id:
                return jsonify([])
            rows = conn.execute("""
                SELECT ex.*, e.name_ar, e.name_en
                FROM excuse_requests ex
                JOIN employees e ON e.id=ex.employee_id
                WHERE ex.employee_id=?
                ORDER BY ex.submitted_at DESC
            """, (emp_id,)).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/excuses', methods=['POST'])
@login_required
def api_excuses_post():
    d       = request.get_json(silent=True) or {}
    emp_id  = session.get('employee_id')
    role    = session.get('role')
    # HR يقدر يرسل لأي موظف
    if role in ('hr', 'manager'):
        emp_id = d.get('employee_id', emp_id)
    if not emp_id:
        return jsonify({'error': 'لم يتم ربط المستخدم بموظف'}), 400
    att_date   = d.get('att_date', '')
    vtype      = d.get('vtype', 'late')
    reason     = (d.get('reason') or '').strip()
    attachment = d.get('attachment', '')
    att_name   = d.get('attachment_name', '')
    if not att_date or not reason:
        return jsonify({'error': 'التاريخ والسبب مطلوبان'}), 400
    conn = get_db()
    try:
        exists = conn.execute(
            "SELECT 1 FROM excuse_requests WHERE employee_id=? AND att_date=? AND vtype=? AND status='pending'",
            (emp_id, att_date, vtype)).fetchone()
        if exists:
            return jsonify({'error': 'يوجد طلب معلق بالفعل لهذا اليوم'}), 400
        conn.execute(
            "INSERT INTO excuse_requests (employee_id, att_date, vtype, reason, attachment, attachment_name) VALUES (?,?,?,?,?,?)",
            (emp_id, att_date, vtype, reason, attachment, att_name))
        conn.commit()
        # إشعار المدراء
        _notify_excuse_submitted(emp_id, att_date, vtype, reason, conn)
        return jsonify({'ok': True, 'msg': 'تم إرسال العذر بنجاح'})
    finally:
        conn.close()

@app.route('/api/excuses/<int:eid>', methods=['PUT'])
@hr_required
def api_excuse_decide(eid):
    d      = request.get_json(silent=True) or {}
    status = d.get('status')
    note   = d.get('note', '')
    if status not in ('approved', 'rejected'):
        return jsonify({'error': 'الحالة يجب أن تكون approved أو rejected'}), 400
    conn = get_db()
    try:
        conn.execute("""
            UPDATE excuse_requests
            SET status=?, decided_by=?, decided_at=datetime('now'), manager_note=?
            WHERE id=?
        """, (status, session['user_id'], note, eid))
        # إذا وافق المدير: احذف المخالفة المرتبطة
        if status == 'approved':
            ex = conn.execute(
                "SELECT * FROM excuse_requests WHERE id=?", (eid,)).fetchone()
            if ex:
                conn.execute("""
                    DELETE FROM violations
                    WHERE employee_id=? AND vio_date=? AND vtype=?
                """, (ex['employee_id'], ex['att_date'], ex['vtype']))
                # إذا كان غياب وتمت الموافقة → حدّث الحضور
                if ex['vtype'] == 'absent':
                    conn.execute("""
                        UPDATE attendance SET status='excused'
                        WHERE employee_id=? AND att_date=?
                    """, (ex['employee_id'], ex['att_date']))
        conn.commit()
        _notify_excuse_decision(eid, status, note, conn)
        return jsonify({'ok': True})
    finally:
        conn.close()

def _notify_excuse_submitted(emp_id, att_date, vtype, reason, conn):
    emp = conn.execute("SELECT * FROM employees WHERE id=?", (emp_id,)).fetchone()
    if not emp: return
    name = emp['name_ar']
    vtype_ar = {'late': 'تأخر', 'early_leave': 'مغادرة مبكرة', 'absent': 'غياب'}.get(vtype, vtype)
    managers = conn.execute(
        "SELECT u.*, e.email AS memail FROM users u LEFT JOIN employees e ON e.id=u.employee_id "
        "WHERE u.role IN ('hr','manager')").fetchall()
    for mgr in managers:
        to = mgr['memail'] or EMAIL_FROM
        if not to: continue
        subj = f"📋 طلب عذر جديد — {name} — {att_date}"
        body = f"""<div style="{_STYLE}">
          <h2 style="color:#3b82f6;margin-bottom:6px">📋 طلب عذر جديد</h2>
          <p>قدّم الموظف <b>{name}</b> عذراً عن <b>{vtype_ar}</b> بتاريخ <b>{att_date}</b>.</p>
          <p><b>السبب:</b> {reason}</p>
          <a href="{SITE_URL}" style="display:inline-block;background:#3b82f6;color:#fff;padding:10px 22px;border-radius:8px;text-decoration:none;font-weight:700;margin-top:10px">
            مراجعة الطلب
          </a>
        </div>"""
        send_email(to, subj, body)

def _notify_excuse_decision(excuse_id, status, note, conn):
    ex  = conn.execute("SELECT * FROM excuse_requests WHERE id=?", (excuse_id,)).fetchone()
    if not ex: return
    emp = conn.execute("SELECT * FROM employees WHERE id=?", (ex['employee_id'],)).fetchone()
    if not emp or not emp['email']: return
    name = emp['name_ar']
    status_ar = 'مقبول ✅' if status == 'approved' else 'مرفوض ❌'
    color = '#16a34a' if status == 'approved' else '#dc2626'
    vtype_ar = {'late': 'تأخر', 'early_leave': 'مغادرة مبكرة', 'absent': 'غياب'}.get(ex['vtype'], ex['vtype'])
    note_row = f"<p><b>ملاحظة المدير:</b> {note}</p>" if note else ''
    subj = f"{'✅' if status=='approved' else '❌'} قرار العذر — {ex['att_date']}"
    body = f"""<div style="{_STYLE}">
      <h2 style="color:{color};margin-bottom:6px">{status_ar} — عذر {vtype_ar}</h2>
      <p>عزيزي/عزيزتي <b>{name}</b>،</p>
      <p>تم <b style="color:{color}">{status_ar}</b> عذرك عن <b>{vtype_ar}</b> بتاريخ <b>{ex['att_date']}</b>.</p>
      {note_row}
    </div>"""
    send_email(emp['email'], subj, body)

# ═══════════════════════════════════════════════════════════
#  LEAVES ROUTES
# ═══════════════════════════════════════════════════════════
LEAVE_NAMES = {
    'annual':   'إجازة سنوية',
    'sick':     'إجازة مرضية',
    'emergency':'إجازة اضطرارية',
    'official': 'إجازة رسمية',
}

@app.route('/api/leaves', methods=['GET'])
@login_required
def api_leaves_get():
    conn = get_db()
    try:
        role   = session.get('role')
        emp_id = session.get('employee_id')
        if role in ('hr', 'manager'):
            rows = conn.execute("""
                SELECT l.*, e.name_ar FROM leaves l
                JOIN employees e ON e.id=l.employee_id
                ORDER BY l.created_at DESC
            """).fetchall()
        else:
            if not emp_id: return jsonify([])
            rows = conn.execute("""
                SELECT l.*, e.name_ar FROM leaves l
                JOIN employees e ON e.id=l.employee_id
                WHERE l.employee_id=? ORDER BY l.created_at DESC
            """, (emp_id,)).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/leaves', methods=['POST'])
@login_required
def api_leaves_post():
    d        = request.get_json(silent=True) or {}
    role     = session.get('role')
    emp_id   = session.get('employee_id')
    if role in ('hr', 'manager'):
        emp_id = d.get('employee_id', emp_id)
    if not emp_id:
        return jsonify({'error': 'لم يتم ربط المستخدم بموظف'}), 400
    leave_type = d.get('leave_type', '')
    start_date = d.get('start_date', '')
    end_date   = d.get('end_date', '')
    notes      = d.get('notes', '')
    if not leave_type or not start_date or not end_date:
        return jsonify({'error': 'نوع الإجازة والتاريخ مطلوبان'}), 400
    try:
        s = date.fromisoformat(start_date)
        e_d = date.fromisoformat(end_date)
        days = (e_d - s).days + 1
        if days <= 0:
            return jsonify({'error': 'تاريخ النهاية يجب أن يكون بعد تاريخ البداية'}), 400
    except ValueError:
        return jsonify({'error': 'تنسيق التاريخ غير صحيح'}), 400

    # إجازة مرضية: تحتاج وثيقة — نقبل الطلب لكن نضع ملاحظة
    sick_doc = d.get('sick_doc', '')
    # HR يوافق مباشرة، الموظف ينتظر موافقة
    init_status = 'approved' if role in ('hr', 'manager') else 'pending'
    approved_by = session['user_id'] if init_status == 'approved' else None

    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO leaves
                (employee_id, leave_type, start_date, end_date, days,
                 status, approved_by, sick_doc, notes)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (emp_id, leave_type, start_date, end_date, days,
              init_status, approved_by, sick_doc, notes))
        conn.commit()
        return jsonify({'ok': True, 'msg': 'تم تسجيل الإجازة', 'days': days})
    finally:
        conn.close()

@app.route('/api/leaves/<int:lid>', methods=['PUT'])
@hr_required
def api_leave_decide(lid):
    d = request.get_json(silent=True) or {}
    status = d.get('status')
    if status not in ('approved', 'rejected'):
        return jsonify({'error': 'الحالة غير صحيحة'}), 400
    conn = get_db()
    try:
        conn.execute("""
            UPDATE leaves SET status=?, approved_by=?, notes=COALESCE(?,notes)
            WHERE id=?
        """, (status, session['user_id'], d.get('notes'), lid))
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True})

@app.route('/api/leaves/<int:lid>', methods=['DELETE'])
@hr_required
def api_leave_delete(lid):
    conn = get_db()
    try:
        conn.execute("DELETE FROM leaves WHERE id=?", (lid,))
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True})

# ═══════════════════════════════════════════════════════════
#  PUBLIC HOLIDAYS ROUTES
# ═══════════════════════════════════════════════════════════
@app.route('/api/holidays', methods=['GET'])
@login_required
def api_holidays_get():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM public_holidays ORDER BY h_date").fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/holidays', methods=['POST'])
@hr_required
def api_holidays_post():
    d    = request.get_json(silent=True) or {}
    hd   = d.get('h_date', '').strip()
    name = d.get('name', '').strip()
    if not hd or not name:
        return jsonify({'error': 'التاريخ والاسم مطلوبان'}), 400
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO public_holidays (h_date, name, created_by) VALUES (?,?,?)",
            (hd, name, session['user_id']))
        conn.commit()
        return jsonify({'ok': True})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'هذا التاريخ مضاف بالفعل'}), 400
    finally:
        conn.close()

@app.route('/api/holidays/<int:hid>', methods=['DELETE'])
@hr_required
def api_holiday_delete(hid):
    conn = get_db()
    try:
        conn.execute("DELETE FROM public_holidays WHERE id=?", (hid,))
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True})

# ═══════════════════════════════════════════════════════════
#  OVERTIME ROUTES
# ═══════════════════════════════════════════════════════════
@app.route('/api/overtime', methods=['GET'])
@login_required
def api_overtime_get():
    conn = get_db()
    try:
        role   = session.get('role')
        emp_id = session.get('employee_id')
        if role in ('hr', 'manager'):
            rows = conn.execute("""
                SELECT ot.*, e.name_ar FROM overtime_requests ot
                JOIN employees e ON e.id=ot.employee_id
                ORDER BY ot.att_date DESC
            """).fetchall()
        else:
            if not emp_id: return jsonify([])
            rows = conn.execute("""
                SELECT ot.*, e.name_ar FROM overtime_requests ot
                JOIN employees e ON e.id=ot.employee_id
                WHERE ot.employee_id=? ORDER BY ot.att_date DESC
            """, (emp_id,)).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/overtime/<int:oid>', methods=['PUT'])
@hr_required
def api_overtime_decide(oid):
    d = request.get_json(silent=True) or {}
    status = d.get('status')
    if status not in ('approved', 'rejected'):
        return jsonify({'error': 'الحالة غير صحيحة'}), 400
    conn = get_db()
    try:
        conn.execute("""
            UPDATE overtime_requests
            SET status=?, decided_by=?, decided_at=datetime('now')
            WHERE id=?
        """, (status, session['user_id'], oid))
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True})

# ═══════════════════════════════════════════════════════════
#  EMPLOYEE SELF-SERVICE ROUTES
# ═══════════════════════════════════════════════════════════
@app.route('/api/my/attendance')
@login_required
def api_my_attendance():
    emp_id = session.get('employee_id')
    if not emp_id:
        return jsonify({'error': 'المستخدم غير مرتبط بموظف'}), 400
    y = request.args.get('year',  date.today().year,  type=int)
    m = request.args.get('month', date.today().month, type=int)
    prefix = f"{y}-{m:02d}-%"
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT a.*, e.name_ar
            FROM attendance a JOIN employees e ON e.id=a.employee_id
            WHERE a.employee_id=? AND a.att_date LIKE ?
            ORDER BY a.att_date DESC
        """, (emp_id, prefix)).fetchall()
        # إضافة معلومة وجود عذر مقدم لكل سجل
        result = []
        for r in rows:
            d = dict(r)
            ex = conn.execute(
                "SELECT status FROM excuse_requests WHERE employee_id=? AND att_date=?",
                (emp_id, r['att_date'])).fetchone()
            d['excuse_status'] = ex['status'] if ex else None
            result.append(d)
    finally:
        conn.close()
    return jsonify(result)

@app.route('/api/my/violations')
@login_required
def api_my_violations():
    emp_id = session.get('employee_id')
    if not emp_id:
        return jsonify({'error': 'المستخدم غير مرتبط بموظف'}), 400
    y = request.args.get('year',  date.today().year,  type=int)
    m = request.args.get('month', date.today().month, type=int)
    prefix = f"{y}-{m:02d}-%"
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT v.*, e.name_ar
            FROM violations v JOIN employees e ON e.id=v.employee_id
            WHERE v.employee_id=? AND v.vio_date LIKE ?
            ORDER BY v.vio_date DESC
        """, (emp_id, prefix)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            ex = conn.execute(
                "SELECT id, status FROM excuse_requests WHERE employee_id=? AND att_date=? AND vtype=?",
                (emp_id, r['vio_date'], r['vtype'])).fetchone()
            d['excuse_id']     = ex['id']     if ex else None
            d['excuse_status'] = ex['status'] if ex else None
            result.append(d)
    finally:
        conn.close()
    return jsonify(result)

@app.route('/api/my/payroll')
@login_required
def api_my_payroll():
    emp_id = session.get('employee_id')
    if not emp_id:
        return jsonify({'error': 'المستخدم غير مرتبط بموظف'}), 400
    y = request.args.get('year',  date.today().year,  type=int)
    m = request.args.get('month', date.today().month, type=int)
    prefix = f"{y}-{m:02d}-%"
    conn = get_db()
    try:
        emp = conn.execute("SELECT * FROM employees WHERE id=?", (emp_id,)).fetchone()
        if not emp:
            return jsonify({'error': 'الموظف غير موجود'}), 404
        emp = dict(emp)
        vios = conn.execute(
            "SELECT * FROM violations WHERE employee_id=? AND vio_date LIKE ? ORDER BY vio_date",
            (emp_id, prefix)).fetchall()
        atts = conn.execute(
            "SELECT * FROM attendance WHERE employee_id=? AND att_date LIKE ? ORDER BY att_date",
            (emp_id, prefix)).fetchall()
        total_ded = sum(v['deduction'] for v in vios)
        gross    = emp['salary'] + emp['housing'] + emp['transport'] + emp['commission']
        gosi_ded = _gosi(emp)
        net      = gross - total_ded - gosi_ded
        return jsonify({
            'employee': emp,
            'year': y, 'month': m,
            'gross': round(gross, 2),
            'deductions': round(total_ded, 2),
            'other_ded': gosi_ded,
            'net': round(net, 2),
            'attendance_days': len([a for a in atts if a['status'] != 'absent']),
            'absent_days': len([a for a in atts if a['status'] == 'absent']),
            'late_days': len([a for a in atts if a['status'] == 'late']),
            'violations': [dict(v) for v in vios],
            'attendance': [dict(a) for a in atts],
        })
    finally:
        conn.close()

# ═══════════════════════════════════════════════════════════
#  ATTENDANCE REQUESTS (طلبات التأخر / الخروج المبكر)
# ═══════════════════════════════════════════════════════════
@app.route('/api/requests', methods=['GET'])
@login_required
def api_requests_get():
    conn = get_db()
    try:
        role   = session.get('role')
        emp_id = session.get('employee_id')
        if role in ('hr', 'manager'):
            rows = conn.execute("""
                SELECT ar.*, e.name_ar FROM attendance_requests ar
                JOIN employees e ON e.id=ar.employee_id
                ORDER BY ar.submitted_at DESC
            """).fetchall()
        else:
            if not emp_id: return jsonify([])
            rows = conn.execute("""
                SELECT ar.*, e.name_ar FROM attendance_requests ar
                JOIN employees e ON e.id=ar.employee_id
                WHERE ar.employee_id=? ORDER BY ar.submitted_at DESC
            """, (emp_id,)).fetchall()
    finally:
        conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/requests', methods=['POST'])
@login_required
def api_requests_post():
    d       = request.get_json(silent=True) or {}
    role    = session.get('role')
    emp_id  = session.get('employee_id')
    if role in ('hr', 'manager'):
        emp_id = d.get('employee_id', emp_id)
    if not emp_id:
        return jsonify({'error': 'المستخدم غير مرتبط بموظف'}), 400
    req_date       = d.get('req_date', '')
    req_type       = d.get('req_type', '')
    reason         = (d.get('reason') or '').strip()
    requested_time = d.get('requested_time', '')
    attachment     = d.get('attachment', '')
    att_name       = d.get('attachment_name', '')
    if not req_date or not req_type or not reason:
        return jsonify({'error': 'التاريخ والنوع والسبب مطلوبة'}), 400
    if req_type not in ('late_arrival', 'early_leave'):
        return jsonify({'error': 'نوع الطلب غير صحيح'}), 400
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO attendance_requests
                (employee_id, req_date, req_type, reason, requested_time,
                 attachment, attachment_name)
            VALUES (?,?,?,?,?,?,?)
        """, (emp_id, req_date, req_type, reason, requested_time, attachment, att_name))
        conn.commit()
        _notify_request_submitted(emp_id, req_date, req_type, reason, conn)
        return jsonify({'ok': True, 'msg': 'تم إرسال الطلب بنجاح'})
    finally:
        conn.close()

@app.route('/api/requests/<int:rid>', methods=['PUT'])
@hr_required
def api_request_decide(rid):
    d    = request.get_json(silent=True) or {}
    status = d.get('status')
    note   = d.get('note', '')
    if status not in ('approved', 'rejected'):
        return jsonify({'error': 'الحالة غير صحيحة'}), 400
    conn = get_db()
    try:
        conn.execute("""
            UPDATE attendance_requests
            SET status=?, decided_by=?, decided_at=datetime('now'), manager_note=?
            WHERE id=?
        """, (status, session['user_id'], note, rid))
        conn.commit()
        _notify_request_decision(rid, status, note, conn)
        return jsonify({'ok': True})
    finally:
        conn.close()

@app.route('/api/requests/<int:rid>/attachment')
@login_required
def api_request_attachment(rid):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT attachment, attachment_name FROM attendance_requests WHERE id=?", (rid,)
        ).fetchone()
    finally:
        conn.close()
    if not row or not row['attachment']:
        return jsonify({'error': 'لا يوجد ملف مرفق'}), 404
    return jsonify({'data': row['attachment'], 'name': row['attachment_name']})

@app.route('/api/excuses/<int:eid>/attachment')
@login_required
def api_excuse_attachment(eid):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT attachment, attachment_name FROM excuse_requests WHERE id=?", (eid,)
        ).fetchone()
    finally:
        conn.close()
    if not row or not row['attachment']:
        return jsonify({'error': 'لا يوجد ملف مرفق'}), 404
    return jsonify({'data': row['attachment'], 'name': row['attachment_name']})

def _notify_request_submitted(emp_id, req_date, req_type, reason, conn):
    emp = conn.execute("SELECT * FROM employees WHERE id=?", (emp_id,)).fetchone()
    if not emp: return
    name = emp['name_ar']
    type_ar = 'تأخر في الحضور' if req_type == 'late_arrival' else 'خروج مبكر'
    managers = conn.execute(
        "SELECT u.*, e.email AS memail FROM users u LEFT JOIN employees e ON e.id=u.employee_id "
        "WHERE u.role IN ('hr','manager')").fetchall()
    for mgr in managers:
        to = mgr['memail'] or EMAIL_FROM
        if not to: continue
        subj = f"📨 طلب {type_ar} — {name} — {req_date}"
        body = f"""<div style="{_STYLE}">
          <h2 style="color:#8b5cf6;margin-bottom:6px">📨 طلب {type_ar}</h2>
          <p>قدّم الموظف <b>{name}</b> طلباً بتاريخ <b>{req_date}</b>.</p>
          <p><b>السبب:</b> {reason}</p>
          <a href="{SITE_URL}" style="display:inline-block;background:#8b5cf6;color:#fff;padding:10px 22px;border-radius:8px;text-decoration:none;font-weight:700;margin-top:10px">
            مراجعة الطلب
          </a>
        </div>"""
        send_email(to, subj, body)

def _notify_request_decision(req_id, status, note, conn):
    req = conn.execute("SELECT * FROM attendance_requests WHERE id=?", (req_id,)).fetchone()
    if not req: return
    emp = conn.execute("SELECT * FROM employees WHERE id=?", (req['employee_id'],)).fetchone()
    if not emp or not emp['email']: return
    type_ar  = 'تأخر في الحضور' if req['req_type'] == 'late_arrival' else 'خروج مبكر'
    status_ar = 'مقبول ✅' if status == 'approved' else 'مرفوض ❌'
    color = '#16a34a' if status == 'approved' else '#dc2626'
    note_row = f"<p><b>ملاحظة:</b> {note}</p>" if note else ''
    subj = f"{'✅' if status=='approved' else '❌'} قرار طلب {type_ar} — {req['req_date']}"
    body = f"""<div style="{_STYLE}">
      <h2 style="color:{color};margin-bottom:6px">{status_ar} — طلب {type_ar}</h2>
      <p>عزيزي/عزيزتي <b>{emp['name_ar']}</b>،</p>
      <p>تم <b style="color:{color}">{status_ar}</b> طلبك بتاريخ <b>{req['req_date']}</b>.</p>
      {note_row}
    </div>"""
    send_email(emp['email'], subj, body)

# ═══════════════════════════════════════════════════════════
#  AUTO-REJECT EXCUSES JOB
# ═══════════════════════════════════════════════════════════
def auto_reject_excuses():
    """رفض تلقائي للعذر والطلبات بعد AUTO_REJECT_DAYS أيام"""
    conn = get_db()
    try:
        cutoff = str(datetime.now() - timedelta(days=AUTO_REJECT_DAYS))
        note   = f'رفض تلقائي — لم يتم الرد خلال {AUTO_REJECT_DAYS} أيام'

        # أعذار
        excuses = conn.execute(
            "SELECT * FROM excuse_requests WHERE status='pending' AND submitted_at<?",
            (cutoff,)).fetchall()
        for ex in excuses:
            conn.execute(
                "UPDATE excuse_requests SET status='rejected', decided_at=datetime('now'), manager_note=? WHERE id=?",
                (note, ex['id']))
            _notify_excuse_decision(ex['id'], 'rejected', note, conn)

        # طلبات الحضور
        reqs = conn.execute(
            "SELECT * FROM attendance_requests WHERE status='pending' AND submitted_at<?",
            (cutoff,)).fetchall()
        for rq in reqs:
            conn.execute(
                "UPDATE attendance_requests SET status='rejected', decided_at=datetime('now'), manager_note=? WHERE id=?",
                (note, rq['id']))
            _notify_request_decision(rq['id'], 'rejected', note, conn)

        conn.commit()
        total = len(excuses) + len(reqs)
        if total:
            logger.info(f"Auto-rejected {total} pending requests")
    except Exception as e:
        logger.error(f"auto_reject_excuses error: {e}")
    finally:
        conn.close()

# ═══════════════════════════════════════════════════════════
#  SCHEDULER
# ═══════════════════════════════════════════════════════════
def scheduled_job():
    logger.info("Scheduler triggered — running daily attendance processing")
    process_day()

# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════
if __name__ == '__main__':
    init_db()
    scheduler = BackgroundScheduler(timezone='Asia/Riyadh')
    scheduler.add_job(scheduled_job, CronTrigger(hour=20, minute=0))
    scheduler.add_job(auto_reject_excuses, CronTrigger(hour=8, minute=0))
    scheduler.start()
    logger.info("Scheduler started — attendance 20:00, auto-reject 08:00 AST")
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
