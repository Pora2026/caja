from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional, Tuple


class AttendanceCoreService:
    def __init__(
        self,
        *,
        db,
        Attendance,
        AttendanceConsumption,
        RotationConfig,
        Vacation,
        CalendarDay,
        employees,
        group_a,
        group_b,
        jornada_min,
        saturday_min,
        rp_limit_min_per_month,
        sick_limit_min_per_month,
        max_consumos,
        novelty_items,
        time_to_min,
        diff_minutes,
        fmt_minutes,
    ):
        self.db = db
        self.Attendance = Attendance
        self.AttendanceConsumption = AttendanceConsumption
        self.RotationConfig = RotationConfig
        self.Vacation = Vacation
        self.CalendarDay = CalendarDay
        self.EMPLOYEES = employees
        self.GROUP_A = group_a
        self.GROUP_B = group_b
        self.JORNADA_MIN = jornada_min
        self.SATURDAY_MIN = saturday_min
        self.RP_LIMIT_MIN_PER_MONTH = rp_limit_min_per_month
        self.SICK_LIMIT_MIN_PER_MONTH = sick_limit_min_per_month
        self.MAX_CONSUMOS = max_consumos
        self.NOVELTY_ITEMS = novelty_items
        self.time_to_min = time_to_min
        self.diff_minutes = diff_minutes
        self.fmt_minutes = fmt_minutes

    def _parse_amount_to_int(self, s: str) -> int:
        s = (s or '').strip()
        s = s.replace('.', '').replace(',', '')
        digits = re.findall(r'\d+', s)
        return int(''.join(digits)) if digits else 0

    def extract_consumptions_from_notes(self, notes: str, max_items: int | None = None):
        if max_items is None:
            max_items = self.MAX_CONSUMOS
        if not notes:
            return []
        txt = str(notes).strip()
        if '$' not in txt:
            return []
        parts = re.split(r'[\/\|,]+', txt)
        out = []
        for p in parts:
            p = p.strip()
            if '$' not in p:
                continue
            m = re.search(r'(?P<item>.*?)(\$)\s*(?P<amt>[\d\.\,]+)', p)
            if not m:
                continue
            item = (m.group('item') or '').strip() or 'Consumo'
            amt = self._parse_amount_to_int(m.group('amt'))
            if amt <= 0:
                continue
            out.append((item[:120], amt))
            if len(out) >= max_items:
                break
        return out

    def extract_consumptions_and_clean_notes(self, notes: str, max_items: int | None = None):
        if max_items is None:
            max_items = self.MAX_CONSUMOS
        if notes is None:
            return [], None
        txt = str(notes).strip()
        if txt == '':
            return [], None
        cons = self.extract_consumptions_from_notes(txt, max_items=max_items)
        if not cons:
            return [], txt
        cleaned_parts = []
        parts = re.split(r'[\/\|,]+', txt)
        for p in parts:
            p = p.strip()
            if '$' in p:
                p2 = re.sub(r'\$\s*[\d\.\,]+', '', p).strip()
                if p2:
                    cleaned_parts.append(p2)
            else:
                if p:
                    cleaned_parts.append(p)
        notes_clean = ' / '.join(cleaned_parts).strip()
        return cons, (notes_clean if notes_clean else None)

    def week_monday(self, d: date) -> date:
        return d - timedelta(days=d.weekday())

    def rotation_config_get(self):
        return self.RotationConfig.query.order_by(self.RotationConfig.id.desc()).first()

    def parse_week0_map(self, s: str) -> dict:
        m = {}
        if not s:
            return m
        parts = [p.strip() for p in s.split(',') if p.strip()]
        for p in parts:
            if ':' in p:
                emp, g = p.split(':', 1)
                emp = emp.strip()
                g = g.strip().upper()
                if emp and g in ('A', 'B'):
                    m[emp] = g
        return m

    def make_week0_map_str(self, m: dict) -> str:
        out = []
        for emp in self.EMPLOYEES:
            g = (m.get(emp) or 'A').upper()
            if g not in ('A', 'B'):
                g = 'A'
            out.append(f'{emp}:{g}')
        return ','.join(out)

    def group_for_employee_on_day(self, emp: str, d: date) -> str:
        cfg = self.rotation_config_get()
        if not cfg or not cfg.week0_map:
            return 'A'
        week0 = self.parse_week0_map(cfg.week0_map)
        base = week0.get(emp, 'A')
        base = base if base in ('A', 'B') else 'A'
        monday = self.week_monday(d)
        monday0 = self.week_monday(cfg.created_at.date())
        weeks = (monday - monday0).days // 7
        if weeks % 2 == 0:
            return base
        return 'B' if base == 'A' else 'A'

    def expected_times_for_group(self, g: str, d: Optional[date] = None):
        if d is not None and d.weekday() == 5:
            if g == 'A':
                return {
                    'morning_in': '07:55',
                    'morning_out': '13:00',
                    'afternoon_in': '',
                    'afternoon_out': '',
                }
            return {
                'morning_in': '',
                'morning_out': '',
                'afternoon_in': '15:55',
                'afternoon_out': '21:00',
            }
        return self.GROUP_A if g == 'A' else self.GROUP_B

    def is_vacation(self, emp: str, d: date) -> bool:
        v = self.Vacation.query.filter(
            self.Vacation.employee == emp,
            self.Vacation.start_day <= d,
            self.Vacation.end_day >= d
        ).first()
        return bool(v)

    def month_range(self, d: date) -> Tuple[date, date]:
        start = d.replace(day=1)
        if start.month == 12:
            end = date(start.year + 1, 1, 1) - timedelta(days=1)
        else:
            end = date(start.year, start.month + 1, 1) - timedelta(days=1)
        return start, end

    def jornada_min_for_day(self, d: date) -> int:
        if d.weekday() == 5:
            return self.SATURDAY_MIN
        return self.JORNADA_MIN

    def get_holiday_type(self, d: date) -> str:
        row = self.CalendarDay.query.filter_by(day=d).first()
        return (row.holiday_type or '') if row else ''

    def set_holiday_type(self, d: date, htype: str):
        row = self.CalendarDay.query.filter_by(day=d).first()
        if not row:
            row = self.CalendarDay(day=d, holiday_type=htype or '')
            self.db.session.add(row)
        else:
            row.holiday_type = htype or ''

    def monthly_used_minutes(self, emp: str, d: date, novelty_name: str) -> int:
        start, end = self.month_range(d)
        q = self.Attendance.query.filter(
            self.Attendance.employee == emp,
            self.Attendance.day >= start,
            self.Attendance.day <= end,
            self.Attendance.novelty == novelty_name
        ).all()
        return sum(int(a.novelty_minutes or 0) for a in q)

    def compute_work_minutes_and_flags(self, a) -> dict:
        emp = a.employee
        d = a.day
        warnings = []
        g = a.group_code or self.group_for_employee_on_day(emp, d)
        exp = self.expected_times_for_group(g, d)
        jornada_min = self.jornada_min_for_day(d)
        hday = (self.get_holiday_type(d) or '').upper().strip()

        if a.novelty == 'Vacaciones' or self.is_vacation(emp, d):
            return {'total_worked_min': jornada_min, 'payable_min': jornada_min, 'tardy_min': 0, 'rp_unpaid_min': 0, 'sick_unpaid_min': 0, 'warnings': ['Vacaciones'], 'group': g, 'exp': exp}
        if a.novelty == 'Inasistencia':
            return {'total_worked_min': 0, 'payable_min': 0, 'tardy_min': 0, 'rp_unpaid_min': 0, 'sick_unpaid_min': 0, 'warnings': ['Inasistencia'], 'group': g, 'exp': exp}
        if a.novelty == 'Delivery':
            return {'total_worked_min': 0, 'payable_min': 0, 'tardy_min': 0, 'rp_unpaid_min': 0, 'sick_unpaid_min': 0, 'warnings': ['Delivery (pago aparte)'], 'group': g, 'exp': exp}

        if a.morning_in or a.morning_out:
            mi = a.morning_in or (exp.get('morning_in') or '')
            mo = a.morning_out or (exp.get('morning_out') or '')
            mm = self.diff_minutes(mi, mo)
        else:
            mm = 0
        if a.afternoon_in or a.afternoon_out:
            ai = a.afternoon_in or (exp.get('afternoon_in') or '')
            ao = a.afternoon_out or (exp.get('afternoon_out') or '')
            ma = self.diff_minutes(ai, ao)
        else:
            ma = 0
        total_worked = 0
        for v in (mm, ma):
            if v is None:
                continue
            if v > 0:
                total_worked += v
        if not (a.morning_in or a.morning_out or a.afternoon_in or a.afternoon_out):
            total_worked = jornada_min

        exp_start = exp.get('morning_in') or exp.get('afternoon_in') or ''
        real_start = a.morning_in or exp_start if exp.get('morning_in') else (a.afternoon_in or exp_start if exp.get('afternoon_in') else None)
        exp_in = self.time_to_min(exp_start)
        real_in = self.time_to_min(real_start or '')
        tardy_min = max(0, real_in - exp_in) if exp_in is not None and real_in is not None else 0
        payable = max(0, int(total_worked))
        rp_unpaid = 0
        sick_unpaid = 0

        if a.novelty == 'Razones particulares':
            req = int(a.novelty_minutes or 0)
            used_excluding_today = max(0, self.monthly_used_minutes(emp, d, 'Razones particulares') - req)
            remaining = max(0, self.RP_LIMIT_MIN_PER_MONTH - used_excluding_today)
            unpaid = max(0, req - remaining)
            rp_unpaid = unpaid
            if unpaid > 0:
                warnings.append(f'! Excede RP: {unpaid} min NO pagos')
            payable = max(0, payable - unpaid)
        if a.novelty == 'Enfermedad':
            req = int(a.novelty_minutes or 0)
            used_excluding_today = max(0, self.monthly_used_minutes(emp, d, 'Enfermedad') - req)
            remaining = max(0, self.SICK_LIMIT_MIN_PER_MONTH - used_excluding_today)
            unpaid = max(0, req - remaining)
            sick_unpaid = unpaid
            if unpaid > 0:
                warnings.append(f'! Excede Enfermedad: {unpaid} min NO pagos')
            payable = max(0, payable - unpaid)
        if a.novelty == 'Curso':
            warnings.append('Curso')
        if a.novelty == 'Tardanza' and tardy_min > 0:
            warnings.append(f'Tardanza: {tardy_min} min')
        if hday == 'LABORABLE':
            payable = int(payable) * 2
            warnings.append('Feriado laborable: paga x2')
        elif hday == 'NO_LABORABLE':
            payable = int(jornada_min)
            warnings.append('Feriado no laborable: paga normal')
        if total_worked < jornada_min and total_worked > 0 and not (a.novelty or '').strip():
            warnings.append('! Menos horas: carga una Novedad')
        if total_worked > jornada_min and not (a.notes or '').strip():
            warnings.append('! Horas extra: carga Nota')
        return {'total_worked_min': total_worked, 'payable_min': payable, 'tardy_min': tardy_min, 'rp_unpaid_min': rp_unpaid, 'sick_unpaid_min': sick_unpaid, 'warnings': warnings, 'group': g, 'exp': exp}

    def consumptions_summary_for_attendance(self, att_id: int) -> Tuple[int, str]:
        cons = (self.AttendanceConsumption.query.filter_by(attendance_id=att_id).order_by(self.AttendanceConsumption.idx.asc()).all())
        total = sum(int(c.amount or 0) for c in cons)
        items = [c.item for c in cons if c.item]
        items_txt = ', '.join(items[:4])
        if len(items) > 4:
            items_txt += '...'
        return total, items_txt

    def attendance_summary_rows(self, start: date, end: date, employee: str, novf: str):
        q = self.Attendance.query.filter(self.Attendance.day >= start, self.Attendance.day <= end)
        if employee and employee != 'ALL':
            q = q.filter(self.Attendance.employee == employee)
        q = q.order_by(self.Attendance.day.desc(), self.Attendance.employee.asc())
        rows = []
        for a in q.all():
            vac = self.is_vacation(a.employee, a.day) or (a.novelty == 'Vacaciones')
            novelty_display = 'Vacaciones' if vac else (a.novelty or '')
            if novf and novf != 'ALL' and novelty_display != novf:
                continue
            g = a.group_code or self.group_for_employee_on_day(a.employee, a.day)
            exp = self.expected_times_for_group(g, a.day)
            mi = a.morning_in or exp['morning_in']
            mo = a.morning_out or exp['morning_out']
            ai = a.afternoon_in or exp['afternoon_in']
            ao = a.afternoon_out or exp['afternoon_out']
            calc = self.compute_work_minutes_and_flags(a)
            cons_total, cons_items = self.consumptions_summary_for_attendance(a.id)
            rows.append({'day': a.day.isoformat(), 'employee': a.employee, 'group': g, 'morning': f'{mi}-{mo}'.strip('-'), 'afternoon': f'{ai}-{ao}'.strip('-'), 'payable': self.fmt_minutes(calc['payable_min']), 'novelty': novelty_display, 'notes': a.notes or '', 'cons_total': cons_total, 'cons_items': cons_items})
        return rows
