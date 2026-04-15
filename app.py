#!/usr/bin/env python3
"""
نظام إدارة الحضور والغياب — TTLock Integration
Flask + SQLite + APScheduler + Gmail SMTP
"""
import os, sqlite3, hashlib, requests, smtplib, logging, io
from datetime import datetime, timedelta, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify, render_template, send_file
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

app = Flask(__name__)
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
EMAIL_FROM = os.getenv('EMAIL_SENDER', '')
EMAIL_PASS = os.getenv('EMAIL_PASSWORD', '')

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
    'flex_hours':  [('hours', 0)],  # خاص بالموظفين المرنين
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
        """)
        conn.commit()
        logger.info("Database initialized OK")
    finally:
        conn.close()

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
          </table></div>"""

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
          </table></div>"""

    elif status == 'absent':
        subj = f"🔴 إشعار غياب — {ds}"
        body = f"""<div style="{_STYLE}">
          <h2 style="color:#dc2626;margin-bottom:6px">🔴 إشعار غياب</h2>
          <p>عزيزي/عزيزتي <b>{name}</b>،</p>
          <p>لم يتم تسجيل حضورك بتاريخ <b>{ds}</b>.</p>
          <p>يرجى مراجعة المسؤول المختص إذا كان هناك عذر مقبول.</p></div>"""
    else:
        return

    send_email(emp['email'], subj, body)

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
                status = 'absent'

            elif emp['work_type'] == 'fixed':
                try:
                    wstart = datetime.strptime(
                        f"{target_date} {emp['work_start']}", "%Y-%m-%d %H:%M")
                    wend   = datetime.strptime(
                        f"{target_date} {emp['work_end']}",   "%Y-%m-%d %H:%M")
                except Exception:
                    wstart = wend = None

                if check_in and wstart:
                    diff = (check_in - wstart).total_seconds() / 60
                    late_min = max(0, int(diff))

                if check_out and wend:
                    diff = (wend - check_out).total_seconds() / 60
                    early_min = max(0, int(diff))

                if check_in and check_out:
                    total_hours = round(
                        (check_out - check_in).total_seconds() / 3600, 2)

                # الحالة: متأخر له الأولوية على المغادرة المبكرة
                if late_min > 0:
                    status = 'late'
                    br = late_bracket(late_min)
                    ptype, pvalue, ded = apply_violation(
                        conn, emp, target_date, 'late', br)
                    if early_min > 0:  # مخالفة مضاعفة
                        apply_violation(conn, emp, target_date,
                                        'early_leave', early_bracket(early_min))
                elif early_min > 0:
                    status = 'early_leave'
                    br = early_bracket(early_min)
                    ptype, pvalue, ded = apply_violation(
                        conn, emp, target_date, 'early_leave', br)
                else:
                    status = 'on_time'

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
            net       = gross - total_ded - emp['other_ded']

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
                    'بدل مواصلات', 'عمولة', 'خصومات', 'استق. أخرى',
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
            gross = emp['salary'] + emp['housing'] + emp['transport'] + emp['commission']
            net   = gross - (vd['d'] or 0) - emp['other_ded']
            result.append({
                **emp,
                'total_ded': round(vd['d'] or 0, 2),
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
    scheduler.start()
    logger.info("Scheduler started — runs daily at 20:00 AST")
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
