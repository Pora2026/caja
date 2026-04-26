from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Optional


class CajaCoreService:
    def __init__(self, *, Shift, ShiftClose, CashExpense, delivery_shift_presets: dict):
        self.Shift = Shift
        self.ShiftClose = ShiftClose
        self.CashExpense = CashExpense
        self.delivery_shift_presets = delivery_shift_presets

    def valid_time_str(self, t: str) -> bool:
        if not t:
            return True
        if len(t) != 5 or t[2] != ":":
            return False
        hh, mm = t.split(":")
        if not (hh.isdigit() and mm.isdigit()):
            return False
        h = int(hh)
        m = int(mm)
        return 0 <= h <= 23 and 0 <= m <= 59

    def time_to_min(self, t: str) -> Optional[int]:
        if not t or not self.valid_time_str(t):
            return None
        hh, mm = map(int, t.split(":"))
        return hh * 60 + mm

    def diff_minutes(self, t_in: str, t_out: str):
        mi = self.time_to_min(t_in)
        mo = self.time_to_min(t_out)
        if mi is None or mo is None:
            return None
        return mo - mi

    def delivery_hours_decimal(self, hour_in: Optional[str], hour_out: Optional[str]) -> float:
        mins = self.diff_minutes(hour_in or "", hour_out or "")
        if mins is None or mins < 0:
            return 0.0
        return round(mins / 60.0, 2)

    def build_delivery_payload(self, shift_row) -> dict:
        payload = {
            "rates": [1500, 2000, 2500, 3000, 2500],
            "qtys": [0, 0, 0, 0, 0],
            "consume_amount": 0,
            "consume_note": "",
            "hour_shift": shift_row.hour_shift or "MORNING",
            "hour_in": shift_row.hour_in or self.delivery_shift_presets["MORNING"]["hour_in"],
            "hour_out": shift_row.hour_out or self.delivery_shift_presets["MORNING"]["hour_out"],
        }
        if shift_row.delivery_data_json:
            try:
                raw = json.loads(shift_row.delivery_data_json)
                if isinstance(raw, dict):
                    payload.update(raw)
            except Exception:
                pass

        hour_shift = (payload.get("hour_shift") or shift_row.hour_shift or "MORNING").strip().upper()
        if hour_shift not in self.delivery_shift_presets:
            hour_shift = "MORNING"
        payload["hour_shift"] = hour_shift

        payload["hour_in"] = payload.get("hour_in") or shift_row.hour_in or self.delivery_shift_presets[hour_shift]["hour_in"]
        payload["hour_out"] = payload.get("hour_out") or shift_row.hour_out or self.delivery_shift_presets[hour_shift]["hour_out"]

        rates = payload.get("rates") if isinstance(payload.get("rates"), list) else None
        if not rates or len(rates) != 5:
            payload["rates"] = [1500, 2000, 2500, 3000, 2500]
        qtys = payload.get("qtys") if isinstance(payload.get("qtys"), list) else None
        if not qtys or len(qtys) != 5:
            payload["qtys"] = [0, 0, 0, 0, 0]

        try:
            payload["consume_amount"] = int(float(payload.get("consume_amount") or 0))
        except Exception:
            payload["consume_amount"] = 0
        payload["consume_note"] = str(payload.get("consume_note") or "")
        payload["qtys"][4] = self.delivery_hours_decimal(payload["hour_in"], payload["hour_out"])
        return payload

    def sanitize_delivery_payload(self, payload):
        if not isinstance(payload, dict):
            payload = {}

        hour_shift = str(payload.get("hour_shift") or "MORNING").strip().upper()
        if hour_shift not in self.delivery_shift_presets:
            hour_shift = "MORNING"

        rates = payload.get("rates")
        if not isinstance(rates, list) or len(rates) != 5:
            rates = [1500, 2000, 2500, 3000, 2500]

        qtys = payload.get("qtys")
        if not isinstance(qtys, list) or len(qtys) != 5:
            qtys = [0, 0, 0, 0, 0]

        def _to_int(v, default=0):
            try:
                return int(float(v or 0))
            except Exception:
                return default

        clean_rates = [_to_int(x, 0) for x in rates[:5]]
        clean_qtys = [_to_int(x, 0) for x in qtys[:5]]

        hour_in = str(payload.get("hour_in") or self.delivery_shift_presets[hour_shift]["hour_in"]).strip()
        hour_out = str(payload.get("hour_out") or self.delivery_shift_presets[hour_shift]["hour_out"]).strip()

        if not self.valid_time_str(hour_in):
            hour_in = self.delivery_shift_presets[hour_shift]["hour_in"]
        if not self.valid_time_str(hour_out):
            hour_out = self.delivery_shift_presets[hour_shift]["hour_out"]

        consume_amount = _to_int(payload.get("consume_amount"), 0)
        consume_note = str(payload.get("consume_note") or "").strip()

        clean_qtys[4] = self.delivery_hours_decimal(hour_in, hour_out)

        return {
            "rates": clean_rates,
            "qtys": clean_qtys,
            "consume_amount": consume_amount,
            "consume_note": consume_note,
            "hour_shift": hour_shift,
            "hour_in": hour_in,
            "hour_out": hour_out,
        }

    def expenses_total(self, shift_id: int) -> int:
        return sum(e.amount for e in self.CashExpense.query.filter_by(shift_id=shift_id).all())

    def cash_final_value(self, s) -> int:
        try:
            sid = getattr(s, "id", None)
            if not sid:
                return 0
            c = self.ShiftClose.query.filter_by(shift_id=sid).first()
            return int(c.ending_cash or 0) if c else 0
        except Exception:
            return 0

    def cash_bruto(self, s, cash_final: Optional[int] = None) -> int:
        retirado = int(s.sales_cash or 0)
        if cash_final is None:
            cash_final = self.cash_final_value(s)
        return retirado + int(cash_final or 0)

    def cash_neto(self, s) -> int:
        return int(s.sales_cash or 0) - int(s.opening_cash or 0)

    def calc_ingreso_bruto(self, s, egresos: int, cash_final: Optional[int] = None) -> int:
        return (
            self.cash_bruto(s, cash_final=cash_final)
            + int(s.sales_mp or 0)
            + int(getattr(s, "sales_pya", 0) or 0)
            + int(getattr(s, "sales_rappi", 0) or 0)
            + int(egresos or 0)
        )

    def calc_ingreso_neto(self, s, egresos: Optional[int] = None, cash_final: Optional[int] = None) -> int:
        if egresos is None:
            try:
                egresos = self.expenses_total(int(s.id))
            except Exception:
                egresos = 0
        return self.calc_ingreso_bruto(s, int(egresos or 0), cash_final=cash_final) - int(egresos or 0)

    def calc_ending_calc(self, cash_final: int, withdrawn: int) -> int:
        return int(cash_final or 0)

    def prev_turn_of(self, day_obj: date, turn_code: str):
        if turn_code == "AFTERNOON":
            return day_obj, "MORNING"
        return day_obj - timedelta(days=1), "AFTERNOON"

    def get_locked_opening_cash(self, day_obj: date, turn_code: str):
        pday, pturn = self.prev_turn_of(day_obj, turn_code)
        prev_shift = self.Shift.query.filter_by(day=pday, turn=pturn).first()
        if not prev_shift or prev_shift.status != "CLOSED":
            return None
        close_row = self.ShiftClose.query.filter_by(shift_id=prev_shift.id).first()
        if not close_row:
            return None

        ec = int(close_row.ending_calc or 0)
        if ec != 0:
            return ec
        er = int(close_row.ending_cash or 0)
        if er != 0:
            return er
        return 0

    def is_placeholder_shift(self, s) -> bool:
        if not s:
            return True
        if (s.status or "").upper() != "CLOSED":
            return False

        nums = [
            int(s.opening_cash or 0),
            int(s.sales_cash or 0),
            int(s.sales_mp or 0),
            int(getattr(s, "sales_pya", 0) or 0),
            int(getattr(s, "sales_rappi", 0) or 0),
            int(getattr(s, "sales_apps", 0) or 0),
        ]
        if any(v != 0 for v in nums):
            return False

        if self.CashExpense.query.filter_by(shift_id=s.id).first():
            return False

        c = self.ShiftClose.query.filter_by(shift_id=s.id).first()
        if c:
            close_nums = [
                int(c.withdrawn_cash or 0),
                int(c.ending_calc or 0),
                int(c.ending_cash or 0),
                int(c.difference or 0),
            ]
            if any(v != 0 for v in close_nums):
                return False
            if (c.note or "").strip():
                return False

        return True

    def get_caja_summary(self, *, limit=200, start: Optional[date]=None, end: Optional[date]=None, turn: str="ALL", responsible: str="ALL", weekday_es=None, responsible_name=None, turn_names=None):
        rows = []
        q = (
            self.Shift.query.join(self.ShiftClose, self.ShiftClose.shift_id == self.Shift.id)
            .filter(self.Shift.status == "CLOSED")
        )
        if start:
            q = q.filter(self.Shift.day >= start)
        if end:
            q = q.filter(self.Shift.day <= end)
        if turn and turn != "ALL":
            q = q.filter(self.Shift.turn == turn)
        if responsible and responsible != "ALL":
            q = q.filter(self.Shift.responsible == responsible)

        q = q.order_by(self.Shift.day.desc(), self.Shift.turn.asc()).limit(limit).all()

        turn_names = turn_names or {}
        weekday_es = weekday_es or (lambda _d: "")
        responsible_name = responsible_name or (lambda v: v or "")

        for s in q:
            c = self.ShiftClose.query.filter_by(shift_id=s.id).first()
            if not c:
                continue
            exp = self.expenses_total(s.id)
            cash_final = int(c.ending_cash or 0)
            bruto = self.calc_ingreso_bruto(s, exp, cash_final=cash_final)
            neto = self.calc_ingreso_neto(s, egresos=exp, cash_final=cash_final)
            rows.append({
                "day": s.day.isoformat(),
                "weekday": weekday_es(s.day),
                "turn_code": s.turn,
                "turn_name": turn_names.get(s.turn, s.turn),
                "responsible": responsible_name(s.responsible),
                "opening_cash": int(s.opening_cash or 0),
                "retirado": int(s.sales_cash or 0),
                "cash_final": cash_final,
                "sales_cash": int(self.cash_bruto(s, cash_final=cash_final)),
                "sales_mp": int(s.sales_mp or 0),
                "sales_pya": int(getattr(s, "sales_pya", 0) or 0),
                "sales_rappi": int(getattr(s, "sales_rappi", 0) or 0),
                "ventas_bruto": int(bruto),
                "ventas_neto": int(neto),
                "expenses": int(exp),
                "withdrawn": int(s.sales_cash or 0),
                "ending_real": int(c.ending_cash or 0),
                "difference": int(c.difference or 0),
                "close_ok": int(c.close_ok or 1),
            })
        return rows
