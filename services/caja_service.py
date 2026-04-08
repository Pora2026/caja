from typing import Optional


def expenses_total(shift_id: int, expense_model) -> int:
    """Suma total de egresos de un turno."""
    return sum(e.amount for e in expense_model.query.filter_by(shift_id=shift_id).all())


def cash_final_value(shift_obj, shift_close_model) -> int:
    """Caja final (efectivo que queda en la caja) guardada en el cierre.
    - Para turnos OPEN (aun sin cierre), devuelve 0.
    """
    try:
        sid = getattr(shift_obj, "id", None)
        if not sid:
            return 0
        c = shift_close_model.query.filter_by(shift_id=sid).first()
        return int(c.ending_cash or 0) if c else 0
    except Exception:
        return 0


def cash_bruto(shift_obj, shift_close_model, cash_final: Optional[int] = None) -> int:
    """Efectivo bruto = Retirado (efectivo total) + Caja final."""
    retirado = int(getattr(shift_obj, "sales_cash", 0) or 0)
    if cash_final is None:
        cash_final = cash_final_value(shift_obj, shift_close_model)
    return retirado + int(cash_final or 0)


def cash_neto(shift_obj) -> int:
    """Efectivo neto = Retirado - Caja inicial.
    Nota: puede ser negativo si Retirado < Caja inicial (ej: faltante / error de carga).
    """
    return int(getattr(shift_obj, "sales_cash", 0) or 0) - int(getattr(shift_obj, "opening_cash", 0) or 0)


def calc_ingreso_bruto(
    shift_obj,
    egresos: int,
    shift_close_model,
    cash_final: Optional[int] = None
) -> int:
    """Ingreso total (bruto) =
       (Efectivo bruto + MP + PedidosYa + Rappi) + Egresos
       donde Efectivo bruto = Retirado + Caja final.
    """
    return (
        cash_bruto(shift_obj, shift_close_model, cash_final=cash_final) +
        int(getattr(shift_obj, "sales_mp", 0) or 0) +
        int(getattr(shift_obj, "sales_pya", 0) or 0) +
        int(getattr(shift_obj, "sales_rappi", 0) or 0) +
        int(egresos or 0)
    )


def calc_ingreso_neto(
    shift_obj,
    shift_close_model,
    expense_model,
    egresos: Optional[int] = None,
    cash_final: Optional[int] = None
) -> int:
    """Ingreso neto = Ingreso total (bruto) - Egresos total."""
    if egresos is None:
        try:
            egresos = expenses_total(int(getattr(shift_obj, "id", 0) or 0), expense_model)
        except Exception:
            egresos = 0
    return calc_ingreso_bruto(
        shift_obj,
        int(egresos or 0),
        shift_close_model,
        cash_final=cash_final
    ) - int(egresos or 0)


def calc_ending_calc(cash_final: int, withdrawn: int) -> int:
    """Compatibilidad legacy.
    Antes: caja final teorica = efectivo bruto - retirado.
    Ahora: la Caja final se carga manualmente, por lo que la 'teorica' coincide con la real.
    """
    return int(cash_final or 0)
