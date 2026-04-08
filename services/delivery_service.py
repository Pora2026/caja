import json
from typing import Optional


DELIVERY_SHIFT_PRESETS = {
    "MORNING": {"hour_in": "08:55", "hour_out": "12:50"},
    "AFTERNOON": {"hour_in": "16:55", "hour_out": "20:50"},
}


def valid_time_str(t: str) -> bool:
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


def delivery_hours_decimal(hour_in: Optional[str], hour_out: Optional[str]) -> float:
    mins = diff_minutes(hour_in or "", hour_out or "")
    if mins is None or mins < 0:
        return 0.0
    return round(mins / 60.0, 2)


def build_delivery_payload(
    delivery_data_json: Optional[str],
    hour_shift: Optional[str],
    hour_in: Optional[str],
    hour_out: Optional[str],
) -> dict:
    payload = {
        "rates": [1500, 2000, 2500, 3000, 2500],
        "qtys": [0, 0, 0, 0, 0],
        "consume_amount": 0,
        "consume_note": "",
        "hour_shift": hour_shift or "MORNING",
        "hour_in": hour_in or DELIVERY_SHIFT_PRESETS["MORNING"]["hour_in"],
        "hour_out": hour_out or DELIVERY_SHIFT_PRESETS["MORNING"]["hour_out"],
    }

    if delivery_data_json:
        try:
            raw = json.loads(delivery_data_json)
            if isinstance(raw, dict):
                payload.update(raw)
        except Exception:
            pass

    shift_code = (payload.get("hour_shift") or hour_shift or "MORNING").strip().upper()
    if shift_code not in DELIVERY_SHIFT_PRESETS:
        shift_code = "MORNING"
    payload["hour_shift"] = shift_code

    payload["hour_in"] = (
        payload.get("hour_in")
        or hour_in
        or DELIVERY_SHIFT_PRESETS[shift_code]["hour_in"]
    )
    payload["hour_out"] = (
        payload.get("hour_out")
        or hour_out
        or DELIVERY_SHIFT_PRESETS[shift_code]["hour_out"]
    )

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
    payload["qtys"][4] = delivery_hours_decimal(payload["hour_in"], payload["hour_out"])
    return payload


def sanitize_delivery_payload(payload: dict) -> dict:
    hour_shift = (payload.get("hour_shift") or "MORNING").strip().upper()
    if hour_shift not in DELIVERY_SHIFT_PRESETS:
        hour_shift = "MORNING"

    hour_in = (payload.get("hour_in") or "").strip()
    hour_out = (payload.get("hour_out") or "").strip()

    if hour_in and not valid_time_str(hour_in):
        raise ValueError("Hora de entrada invalida")
    if hour_out and not valid_time_str(hour_out):
        raise ValueError("Hora de salida invalida")

    rates = payload.get("rates") if isinstance(payload.get("rates"), list) else [1500, 2000, 2500, 3000, 2500]
    qtys = payload.get("qtys") if isinstance(payload.get("qtys"), list) else [0, 0, 0, 0, 0]
    rates = (rates + [0, 0, 0, 0, 0])[:5]
    qtys = (qtys + [0, 0, 0, 0, 0])[:5]

    safe_rates = []
    for v in rates:
        try:
            safe_rates.append(int(float(v or 0)))
        except Exception:
            safe_rates.append(0)

    hours_qty = delivery_hours_decimal(hour_in, hour_out)
    safe_qtys = []
    for i, v in enumerate(qtys):
        if i == 4:
            safe_qtys.append(hours_qty)
        else:
            try:
                safe_qtys.append(int(float(v or 0)))
            except Exception:
                safe_qtys.append(0)

    try:
        consume_amount = int(float(payload.get("consume_amount") or 0))
    except Exception:
        consume_amount = 0

    consume_note = str(payload.get("consume_note") or "")[:200]

    return {
        "rates": safe_rates,
        "qtys": safe_qtys,
        "consume_amount": consume_amount,
        "consume_note": consume_note,
        "hour_shift": hour_shift,
        "hour_in": hour_in,
        "hour_out": hour_out,
    }
