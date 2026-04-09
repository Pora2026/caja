from __future__ import annotations

from datetime import date, timedelta
from typing import Callable, Optional


def prev_turn_of(day_obj: date, turn_code: str):
    if turn_code == "AFTERNOON":
        return day_obj, "MORNING"
    return day_obj - timedelta(days=1), "AFTERNOON"


def get_locked_opening_cash(day_obj: date, turn_code: str, Shift, ShiftClose):
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


def is_placeholder_shift(s, CashExpense, ShiftClose) -> bool:
    """Turno cerrado en cero, sin egresos ni cierre real."""
    if not s:
        return True

    if (s.status or "").upper() != "CLOSED":
        return False

    nums = [
        int(s.opening_cash or 0),
        int(s.sales_cash or 0),
        int(s.sales_mp or 0),
        int(s.sales_pya or 0),
        int(s.sales_rappi or 0),
        int(getattr(s, "sales_apps", 0) or 0),
    ]
    if any(v != 0 for v in nums):
        return False

    if CashExpense.query.filter_by(shift_id=s.id).first():
        return False

    c = ShiftClose.query.filter_by(shift_id=s.id).first()
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


def get_caja_summary(
    db,
    Shift,
    ShiftClose,
    CashExpense,
    *,
    limit: int = 200,
    start: Optional[date] = None,
    end: Optional[date] = None,
    turn: str = "ALL",
    responsible: str = "ALL",
    weekday_es: Callable[[date], str],
    responsible_name: Callable[[str], str],
    turn_names: dict,
    expenses_total: Callable,
    calc_ingreso_bruto: Callable,
    calc_ingreso_neto: Callable,
    cash_bruto: Callable,
):
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
        exp = expenses_total(s.id, CashExpense)
        cash_final = int(c.ending_cash or 0)

        bruto = calc_ingreso_bruto(
            s,
            exp,
            ShiftClose,
            cash_final=cash_final,
        )

        neto = calc_ingreso_neto(
            s,
            ShiftClose,
            CashExpense,
            egresos=exp,
            cash_final=cash_final,
        )

        sales_cash_bruto = cash_bruto(
            s,
            ShiftClose,
            cash_final=cash_final,
        )

        rows.append({
            "day": s.day.isoformat(),
            "weekday": weekday_es(s.day),
            "turn_code": s.turn,
            "turn_name": turn_names.get(s.turn, s.turn),
            "responsible": responsible_name(s.responsible),
            "opening_cash": int(s.opening_cash or 0),
            "retirado": int(s.sales_cash or 0),
            "cash_final": cash_final,
            "sales_cash": int(sales_cash_bruto),
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