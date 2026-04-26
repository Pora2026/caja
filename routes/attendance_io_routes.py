from __future__ import annotations

def register_attendance_io_routes(
    app,
    *,
    db,
    login_required,
    render_template_string,
    send_file,
    request,
    json_module,
    BytesIO,
    Workbook,
    load_workbook,
    datetime_cls,
    date_cls,
    parse_range_params,
    attendance_summary_rows,
    backup_caja_local_y_drive,
    safe_int,
    EMPLOYEES,
    MAX_CONSUMOS,
    NOVELTY_ITEMS,
    Attendance,
    AttendanceConsumption,
    RotationConfig,
    Vacation,
    CalendarDay,
    set_holiday_type,
    extract_consumptions_and_clean_notes,
    BASE_CSS,
):
    IMPORT_ATT_HTML = """
<!doctype html>
<html lang="es">
<head><meta charset="utf-8"><title>Import Asistencia</title>{{ base_css|safe }}</head>
<body style="max-width:950px;">
  <a href="{{ url_for('asistencia') }}"><- Volver</a>
  <h2>Importar datos (Excel) - Asistencia</h2>

  <div class="box" style="margin-bottom:12px;">
    <b>Formato:</b> hoja <code>attendance</code> (y opcional <code>consumptions</code>).<br>
    Columnas minimas: <code>day</code>, <code>employee</code>, <code>group</code>, <code>morning_in</code>, <code>morning_out</code>,
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
        <option value="skip" selected>Si existe el dia/empleado, NO tocar (skip)</option>
        <option value="replace">Si existe el dia/empleado, REEMPLAZAR (replace)</option>
      </select>
    </div>
    <br>
    <button class="btn">Importar</button>
  </form>
</body>
</html>
"""

    IMPORT_ATT_JSON_HTML = """
<!doctype html>
<html lang="es">
<head><meta charset="utf-8"><title>Import Asistencia (JSON)</title>{{ base_css|safe }}</head>
<body style="max-width:950px;">
  <a href="{{ url_for('asistencia') }}"><- Volver</a>
  <h2>Importar Backup JSON - Asistencia</h2>

  <div class="box" style="margin-bottom:12px;">
    <b>Formato:</b> archivo JSON exportado por "Exportar JSON".<br>
    <b>Modo skip:</b> si existe (dia+empleado), no lo toca.<br>
    <b>Modo replace:</b> reemplaza (dia+empleado) y recrea consumos.<br>
    <span class="muted">Ademas: si el JSON trae vacaciones / feriados / rotacion, se "mergea" (upsert simple).</span>
  </div>

  {% if msg %}<div class="box" style="border-color:#7ad2a4;background:#dff5e6;"><b>{{msg}}</b></div>{% endif %}
  {% if err %}<div class="box" style="border-color:#ffb866;background:#ffe6d6;"><b>Error:</b> {{err}}</div>{% endif %}

  <form method="post" enctype="multipart/form-data" style="margin-top:12px;">
    <div class="box">
      <label><b>Archivo JSON</b></label><br>
      <input type="file" name="file" accept=".json" required>
      <br><br>
      <label><b>Modo</b></label><br>
      <select name="mode">
        <option value="skip" selected>Si existe el dia/empleado, NO tocar (skip)</option>
        <option value="replace">Si existe el dia/empleado, REEMPLAZAR (replace)</option>
      </select>
    </div>
    <br>
    <button class="btn">Importar JSON</button>
  </form>
</body>
</html>
"""

    def _rotation_config_get():
        return RotationConfig.query.order_by(RotationConfig.id.desc()).first()

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
        ws.append(["Fecha","Empleado","Grupo","Manana","Tarde","Horas pagas","Novedad","Notas","Consumos total","Consumos items"])

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
        return send_file(
            bio,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @app.route("/asistencia/export/json")
    @login_required
    def att_export_json():
        emp = request.args.get("emp") or "ALL"
        novf = request.args.get("novf") or "ALL"
        start_date, end_date = parse_range_params(request.args.get("start"), request.args.get("end"))

        data = attendance_summary_rows(start_date, end_date, emp, novf)

        q = Attendance.query.filter(Attendance.day >= start_date, Attendance.day <= end_date)
        if emp != "ALL":
            q = q.filter(Attendance.employee == emp)

        attendances = []
        for a in q.order_by(Attendance.day.asc(), Attendance.employee.asc()).all():
            cons = (
                AttendanceConsumption.query
                .filter_by(attendance_id=a.id)
                .order_by(AttendanceConsumption.idx.asc())
                .all()
            )
            attendances.append({
                "day": a.day.isoformat(),
                "employee": a.employee,
                "group_code": a.group_code,
                "mode": a.mode,
                "morning_in": a.morning_in,
                "morning_out": a.morning_out,
                "afternoon_in": a.afternoon_in,
                "afternoon_out": a.afternoon_out,
                "novelty": a.novelty,
                "novelty_minutes": int(a.novelty_minutes or 0),
                "notes": a.notes,
                "consumptions": [
                    {"idx": int(c.idx), "item": c.item, "amount": int(c.amount or 0)}
                    for c in cons
                ],
            })

        rc = _rotation_config_get()
        payload = {
            "type": "asistencia_backup",
            "version": 1,
            "exported_at": datetime_cls.utcnow().isoformat(),
            "range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
            "filters": {"emp": emp, "novf": novf},
            "attendances": attendances,
            "summary": data,
            "rotation_config": (
                {
                    "week0_map": rc.week0_map,
                    "created_by": rc.created_by,
                    "created_at": rc.created_at.isoformat(),
                } if rc else None
            ),
            "vacations": [
                {"employee": v.employee, "start_day": v.start_day.isoformat(), "end_day": v.end_day.isoformat()}
                for v in Vacation.query.order_by(Vacation.employee.asc(), Vacation.start_day.asc()).all()
            ],
            "calendar_days": [
                {"day": cd.day.isoformat(), "holiday_type": cd.holiday_type or ""}
                for cd in CalendarDay.query.order_by(CalendarDay.day.asc()).all()
            ],
        }

        bio = BytesIO()
        bio.write(json_module.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
        bio.seek(0)
        filename = f"asistencia_backup_{start_date.isoformat()}_{end_date.isoformat()}.json"
        return send_file(bio, as_attachment=True, download_name=filename, mimetype="application/json")

    @app.route("/import/asistencia", methods=["GET", "POST"])
    @login_required
    def import_asistencia():
        err = None
        msg = None
        if request.method == "POST":
            mode = (request.form.get("mode") or "skip").strip()
            f = request.files.get("file")
            if not f:
                err = "No se recibio archivo."
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
                        if not r or all(v is None or str(v).strip() == "" for v in r):
                            continue

                        day_v = r[idx["day"]]
                        if isinstance(day_v, datetime_cls):
                            day_v = day_v.date()
                        elif isinstance(day_v, date_cls):
                            pass
                        else:
                            day_v = date_cls.fromisoformat(str(day_v).strip()[:10])

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

                        raw_notes_cell = r[idx["notes"]]
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
                            notes=notes_clean,
                        )
                        db.session.add(a)
                        db.session.flush()

                        for i, (item, amount) in enumerate(cons, start=1):
                            db.session.add(
                                AttendanceConsumption(
                                    attendance_id=a.id,
                                    idx=i,
                                    item=item,
                                    amount=amount,
                                )
                            )

                        imported += 1

                    db.session.commit()
                    backup_caja_local_y_drive()
                    msg = f"Import OK. Registros: {imported}. Reemplazados: {replaced}."
                except Exception as ex:
                    db.session.rollback()
                    err = str(ex)

        return render_template_string(IMPORT_ATT_HTML, base_css=BASE_CSS, err=err, msg=msg)

    @app.route("/import/asistencia/json", methods=["GET", "POST"])
    @login_required
    def import_asistencia_json():
        err = None
        msg = None

        if request.method == "POST":
            mode = (request.form.get("mode") or "skip").strip()
            f = request.files.get("file")
            if not f:
                err = "No se recibio archivo."
            else:
                try:
                    payload = json_module.load(f)
                    if not isinstance(payload, dict) or payload.get("type") != "asistencia_backup":
                        raise ValueError("JSON invalido: no parece un backup de Asistencia.")

                    attendances = payload.get("attendances") or []

                    imported = 0
                    replaced = 0
                    skipped = 0

                    for a in attendances:
                        dd = (a.get("day") or "").strip()
                        emp = (a.get("employee") or "").strip()
                        if not dd or emp not in EMPLOYEES:
                            continue

                        day_obj = date_cls.fromisoformat(dd)

                        existing = Attendance.query.filter_by(day=day_obj, employee=emp).first()
                        if existing:
                            if mode == "skip":
                                skipped += 1
                                continue
                            AttendanceConsumption.query.filter_by(attendance_id=existing.id).delete()
                            db.session.delete(existing)
                            db.session.flush()
                            replaced += 1

                        grp = (a.get("group_code") or "A").strip().upper()
                        if grp not in ("A","B"):
                            grp = "A"

                        nov = (a.get("novelty") or "").strip()
                        if nov and nov not in NOVELTY_ITEMS:
                            nov = ""

                        row = Attendance(
                            day=day_obj,
                            employee=emp,
                            group_code=grp,
                            mode=(a.get("mode") or "MANUAL"),
                            morning_in=a.get("morning_in"),
                            morning_out=a.get("morning_out"),
                            afternoon_in=a.get("afternoon_in"),
                            afternoon_out=a.get("afternoon_out"),
                            novelty=nov or None,
                            novelty_minutes=int(a.get("novelty_minutes") or 0),
                            notes=(a.get("notes") or None),
                        )
                        db.session.add(row)
                        db.session.flush()

                        cons = a.get("consumptions") or []
                        for c in cons:
                            idx = int(c.get("idx") or 0)
                            if idx <= 0:
                                continue
                            db.session.add(AttendanceConsumption(
                                attendance_id=row.id,
                                idx=idx,
                                item=(c.get("item") or None),
                                amount=int(c.get("amount") or 0),
                            ))

                        imported += 1

                    rc = payload.get("rotation_config")
                    if rc and isinstance(rc, dict) and rc.get("week0_map"):
                        db.session.add(RotationConfig(
                            week0_map=str(rc.get("week0_map")),
                            created_by=(rc.get("created_by") or "import-json"),
                        ))

                    vacs = payload.get("vacations") or []
                    for v in vacs:
                        emp = (v.get("employee") or "").strip()
                        sd = (v.get("start_day") or "").strip()
                        ed = (v.get("end_day") or "").strip()
                        if emp not in EMPLOYEES or not sd or not ed:
                            continue
                        sd2 = date_cls.fromisoformat(sd)
                        ed2 = date_cls.fromisoformat(ed)
                        exists = Vacation.query.filter_by(employee=emp, start_day=sd2, end_day=ed2).first()
                        if not exists:
                            db.session.add(Vacation(employee=emp, start_day=sd2, end_day=ed2))

                    cds = payload.get("calendar_days") or []
                    for cd in cds:
                        dd = (cd.get("day") or "").strip()
                        if not dd:
                            continue
                        d2 = date_cls.fromisoformat(dd)
                        ht = (cd.get("holiday_type") or "").strip().upper()
                        if ht not in ("", "LABORABLE", "NO_LABORABLE"):
                            ht = ""
                        set_holiday_type(d2, ht)

                    db.session.commit()
                    backup_caja_local_y_drive()
                    msg = f"Import JSON OK. Registros: {imported}. Reemplazados: {replaced}. Omitidos (skip): {skipped}."
                except Exception as ex:
                    db.session.rollback()
                    err = str(ex)

        return render_template_string(IMPORT_ATT_JSON_HTML, base_css=BASE_CSS, err=err, msg=msg)
