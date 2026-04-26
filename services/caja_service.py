from typing import Optional


def expenses_total(shift_id: int, expense_model) -> int:
    return sum(int(e.amount or 0) for e in expense_model.query.filter_by(shift_id=shift_id).all())


def cash_final_value(s, shift_close_model) -> int:
    try:
        c = shift_close_model.query.filter_by(shift_id=s.id).first()
        return int(c.ending_cash or 0) if c else 0
    except Exception:
        return 0


def cash_bruto(s, shift_close_model, cash_final: Optional[int] = None) -> int:
    retirado = int(getattr(s, "sales_cash", 0) or 0)
    if cash_final is None:
        cash_final = cash_final_value(s, shift_close_model)
    return retirado + int(cash_final or 0)


def calc_ingreso_bruto(s, egresos: int, shift_close_model, cash_final: Optional[int] = None) -> int:
    return (
        cash_bruto(s, shift_close_model, cash_final)
        + int(getattr(s, "sales_mp", 0) or 0)
        + int(getattr(s, "sales_pya", 0) or 0)
        + int(getattr(s, "sales_rappi", 0) or 0)
        + int(egresos or 0)
    )


def calc_ingreso_neto(s, shift_close_model, expense_model, egresos=None, cash_final=None) -> int:
    if egresos is None:
        egresos = expenses_total(s.id, expense_model)

    return calc_ingreso_bruto(
        s,
        egresos,
        shift_close_model,
        cash_final
    ) - int(egresos or 0)


def calc_ending_calc(cash_final: int, withdrawn: int) -> int:
    return int(cash_final or 0)