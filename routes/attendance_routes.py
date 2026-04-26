from __future__ import annotations

from datetime import date
from typing import Optional

ATT_CONFIG_HTML = """
<!doctype html>
<html lang="es">
<head><meta charset="utf-8"><title>Config Asistencia</title>{{ base_css|safe }}</head>
<body style="max-width:1000px;">
  <a href="{{ url_for('asistencia') }}"><- Volver</a>
  <h2>Configurar grupos / vacaciones</h2>

  <div class="box">
    <b>Horarios por grupo</b><br>
    <span class="muted">
      <b>Grupo A:</b> {{ga.morning_in}}-{{ga.morning_out}} / {{ga.afternoon_in}}-{{ga.afternoon_out}}<br>
      <b>Grupo B:</b> {{gb.morning_in}}-{{gb.morning_out}} / {{gb.afternoon_in}}-{{gb.afternoon_out}}
    </span>
  </div>

  {% if msg %}<div class="box" style="border-color:#7ad2a4;background:#dff5e6;"><b>{{msg}}</b></div>{% endif %}
  {% if err %}<div class="box" style="border-color:#ffb866;background:#ffe6d6;"><b>Error:</b> {{err}}</div>{% endif %}

  <h3>Rotacion semanal (A/B)</h3>
  <div class="box">
    <p class="muted">
      Rotacion automatica semanal (AB).<br>
      {% if cfg_exists %}
        <b>Edicion:</b> solo Admin puede modificar configuracion existente.
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
      <button class="btn" {% if lock_rotation %}disabled{% endif %}>Guardar rotacion</button>
      {% if lock_rotation %}
        <div class="muted">Solo Admin puede editar esta configuracion.</div>
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
      <tr><th>Empleado</th><th>Desde</th><th>Hasta</th><th>Accion</th></tr>
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
    <div class="muted"><a href="{{ url_for('home') }}"><- Volver al menu</a></div>
  </div>
    <div class="row">
      <a class="btn" href="{{ url_for('att_export_excel', emp=emp, start=start, end=end, novf=novf) }}">Exportar Excel</a>
      <a class="btn" href="{{ url_for('att_export_json', emp=emp, start=start, end=end, novf=novf) }}">Exportar JSON</a>

      <a class="btn" href="{{ url_for('import_asistencia') }}">Importar Excel</a>
      <a class="btn" href="{{ url_for('import_asistencia_json') }}">Importar JSON</a>

      <a class="btn" href="{{ url_for('att_config') }}">Configurar grupos / vacaciones</a>
    </div>
</div>

<div class="box" style="margin-top:10px;">
  <b>Horarios por grupo</b><br>
  <span class="muted">
    <b>Grupo A:</b> {{ga.morning_in}}-{{ga.morning_out}} / {{ga.afternoon_in}}-{{ga.afternoon_out}} &nbsp;&nbsp;|&nbsp;&nbsp;
    <b>Grupo B:</b> {{gb.morning_in}}-{{gb.morning_out}} / {{gb.afternoon_in}}-{{gb.afternoon_out}}
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
      <option value="NO_LABORABLE" {% if hday=='NO_LABORABLE' %}selected{% endif %}>Feriado No laborable (se paga el dia)</option>
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
    <th colspan="3">Manana</th>
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
    <th>Manana</th>
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


def register_attendance_routes(
    app,
    *,
    db,
    login_required,
    current_user,
    render_template_string,
    request,
    redirect,
    url_for,
    backup_caja_local_y_drive,
    parse_day_param,
    parse_range_params,
    safe_int,
    safe_float,
    valid_time_str,
    Attendance,
    AttendanceConsumption,
    RotationConfig,
    Vacation,
    EMPLOYEES,
    MAX_CONSUMOS,
    NOVELTY_ITEMS,
    GROUP_A,
    GROUP_B,
    attendance_service,
    BASE_CSS,
):
    @app.route('/asistencia/config', methods=['GET', 'POST'])
    @login_required
    def att_config():
        u = current_user()
        err = None
        msg = None
        cfg = attendance_service.rotation_config_get()
        cfg_exists = bool(cfg and cfg.week0_map)
        week0 = attendance_service.parse_week0_map(cfg.week0_map) if cfg_exists else {}
        lock_rotation = cfg_exists and (u.role != 'admin')

        if request.method == 'POST':
            action = (request.form.get('action') or '').strip()
            try:
                if action == 'set_rotation':
                    if lock_rotation:
                        raise ValueError('Solo Admin puede editar la rotacion una vez creada.')
                    m = {}
                    for e in EMPLOYEES:
                        g = (request.form.get(f'g_{e}') or 'A').strip().upper()
                        if g not in ('A', 'B'):
                            g = 'A'
                        m[e] = g
                    if not cfg:
                        cfg = RotationConfig(week0_map=attendance_service.make_week0_map_str(m), created_by=u.username)
                        db.session.add(cfg)
                    else:
                        cfg.week0_map = attendance_service.make_week0_map_str(m)
                        cfg.created_by = cfg.created_by or u.username
                    db.session.commit()
                    backup_caja_local_y_drive()
                    msg = 'Rotacion guardada.'
                elif action == 'add_vac':
                    emp = (request.form.get('emp') or '').strip()
                    start = date.fromisoformat(request.form.get('start'))
                    end = date.fromisoformat(request.form.get('end'))
                    if emp not in EMPLOYEES:
                        raise ValueError('Empleado invalido.')
                    if start > end:
                        start, end = end, start
                    db.session.add(Vacation(employee=emp, start_day=start, end_day=end))
                    db.session.commit()
                    backup_caja_local_y_drive()
                    msg = 'Vacaciones agregadas.'
                elif action == 'del_vac':
                    vid = int(request.form.get('id') or 0)
                    v = db.session.get(Vacation, vid)
                    if v:
                        db.session.delete(v)
                        db.session.commit()
                        backup_caja_local_y_drive()
                    msg = 'Vacaciones borradas.'
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
            msg=msg,
        )

    @app.route('/asistencia', methods=['GET', 'POST'])
    @login_required
    def asistencia():
        day_obj, d = parse_day_param(request.args.get('d') or request.form.get('d'))
        err = None
        msg = None
        empf = request.args.get('emp') or 'ALL'
        novf = request.args.get('novf') or 'ALL'
        start_date, end_date = parse_range_params(request.args.get('start'), request.args.get('end'))
        start = start_date.isoformat()
        end = end_date.isoformat()
        hday = attendance_service.get_holiday_type(day_obj)

        if request.method == 'POST' and (request.form.get('action') or '').strip() == 'set_holiday':
            try:
                h = (request.form.get('hday') or '').strip().upper()
                if h not in ('', 'LABORABLE', 'NO_LABORABLE'):
                    h = ''
                attendance_service.set_holiday_type(day_obj, h)
                db.session.commit()
                backup_caja_local_y_drive()
                msg = 'Feriado aplicado.'
                return redirect(url_for('asistencia', d=d, emp=empf, start=start, end=end, novf=novf))
            except Exception as ex:
                db.session.rollback()
                err = str(ex)

        if request.method == 'POST' and err is None:
            try:
                for emp_name in EMPLOYEES:
                    key = attendance_service.emp_key(emp_name)
                    row = Attendance.query.filter_by(day=day_obj, employee=emp_name).first()
                    if not row:
                        row = Attendance(day=day_obj, employee=emp_name, mode='AUTO')
                        db.session.add(row)
                        db.session.flush()

                    mode = (request.form.get(f'{key}_mode') or 'AUTO').strip().upper()
                    if mode not in ('AUTO', 'MANUAL'):
                        mode = 'AUTO'
                    g = (request.form.get(f'{key}_group') or '').strip().upper()
                    if g not in ('A', 'B'):
                        g = attendance_service.group_for_employee_on_day(emp_name, day_obj)
                    row.mode = mode
                    row.group_code = g
                    exp = attendance_service.expected_times_for_group(g, day_obj)
                    vac = attendance_service.is_vacation(emp_name, day_obj)
                    nov = (request.form.get(f'{key}_nov') or '').strip()
                    if nov not in NOVELTY_ITEMS:
                        nov = ''
                    row.novelty = nov or None

                    def get_post_time(field_name: str) -> Optional[str]:
                        v = (request.form.get(field_name) or '').strip()
                        if v and not valid_time_str(v):
                            raise ValueError(f'Hora invalida en {emp_name}: {v}')
                        return v or None

                    mi_post = get_post_time(f'{key}_mi')
                    mo_post = get_post_time(f'{key}_mo')
                    ai_post = get_post_time(f'{key}_ai')
                    ao_post = get_post_time(f'{key}_ao')
                    if vac or nov == 'Vacaciones':
                        row.morning_in = exp.get('morning_in')
                        row.morning_out = exp.get('morning_out')
                        row.afternoon_in = exp.get('afternoon_in')
                        row.afternoon_out = exp.get('afternoon_out')
                    elif nov == 'Delivery':
                        row.morning_in = None
                        row.morning_out = None
                        row.afternoon_in = None
                        row.afternoon_out = None
                    else:
                        if mode == 'AUTO':
                            row.morning_in = mi_post or row.morning_in or exp.get('morning_in')
                            row.morning_out = mo_post or row.morning_out or exp.get('morning_out')
                            row.afternoon_in = ai_post or row.afternoon_in or exp.get('afternoon_in')
                            row.afternoon_out = ao_post or row.afternoon_out or exp.get('afternoon_out')
                        else:
                            row.morning_in = mi_post
                            row.morning_out = mo_post
                            row.afternoon_in = ai_post
                            row.afternoon_out = ao_post

                    novx = (request.form.get(f'{key}_novx') or '').strip()
                    if nov == 'Razones particulares':
                        m = safe_int(novx) or 0
                        row.novelty_minutes = max(0, int(m))
                    elif nov == 'Enfermedad':
                        h = safe_float(novx) or 0.0
                        row.novelty_minutes = int(round(max(0.0, h) * 60.0))
                    else:
                        row.novelty_minutes = 0
                    row.notes = (request.form.get(f'{key}_notes') or '').strip() or None
                    calc = attendance_service.compute_work_minutes_and_flags(row)
                    jornada_min = attendance_service.jornada_min_for_day(day_obj)
                    if not (vac or nov == 'Vacaciones' or nov == 'Inasistencia'):
                        if calc['total_worked_min'] < jornada_min and not (row.novelty or ''):
                            raise ValueError(f"{emp_name}: si trabajo menos de {attendance_service.fmt_minutes(jornada_min)} debe cargar Novedad.")
                        if calc['total_worked_min'] > jornada_min and not (row.notes and row.notes.strip()):
                            raise ValueError(f"{emp_name}: si trabajo mas de {attendance_service.fmt_minutes(jornada_min)} debe justificar en Notas.")
                    db.session.flush()
                    AttendanceConsumption.query.filter_by(attendance_id=row.id).delete()
                    qty = safe_int(request.form.get(f'{key}_cq')) or 0
                    qty = max(0, min(qty, MAX_CONSUMOS))
                    for i in range(1, qty + 1):
                        item = (request.form.get(f'{key}_ci{i}') or '').strip() or None
                        amount = safe_int(request.form.get(f'{key}_cm{i}'))
                        db.session.add(AttendanceConsumption(attendance_id=row.id, idx=i, item=item, amount=amount))

                db.session.commit()
                backup_caja_local_y_drive()
                msg = 'Guardado.'
                return redirect(url_for('asistencia', d=d, emp=empf, start=start, end=end, novf=novf))
            except Exception as ex:
                db.session.rollback()
                err = str(ex)

        hday = attendance_service.get_holiday_type(day_obj)
        rows = []
        auto_fix_commit = False
        for emp_name in EMPLOYEES:
            key = attendance_service.emp_key(emp_name)
            row = Attendance.query.filter_by(day=day_obj, employee=emp_name).first()
            if not row:
                row = Attendance(day=day_obj, employee=emp_name, mode='AUTO')
                db.session.add(row)
                db.session.flush()
                db.session.commit()
            g = row.group_code or attendance_service.group_for_employee_on_day(emp_name, day_obj)
            exp = attendance_service.expected_times_for_group(g, day_obj)
            if (row.mode or '').upper() == 'MANUAL':
                cons_cnt = AttendanceConsumption.query.filter_by(attendance_id=row.id).count()
                if (not (row.novelty or '').strip()) and (not (row.notes or '').strip()) and cons_cnt == 0:
                    def _eq(a_, b_):
                        return (a_ or '') == (b_ or '')
                    if (_eq(row.morning_in, exp.get('morning_in')) and _eq(row.morning_out, exp.get('morning_out')) and _eq(row.afternoon_in, exp.get('afternoon_in')) and _eq(row.afternoon_out, exp.get('afternoon_out'))):
                        row.mode = 'AUTO'
                        auto_fix_commit = True
            vac = attendance_service.is_vacation(emp_name, day_obj) or (row.novelty == 'Vacaciones')
            mi_preset = (row.mode or 'AUTO') == 'AUTO' and (row.morning_in is None) and not vac and bool(exp.get('morning_in'))
            mo_preset = (row.mode or 'AUTO') == 'AUTO' and (row.morning_out is None) and not vac and bool(exp.get('morning_out'))
            ai_preset = (row.mode or 'AUTO') == 'AUTO' and (row.afternoon_in is None) and not vac and bool(exp.get('afternoon_in'))
            ao_preset = (row.mode or 'AUTO') == 'AUTO' and (row.afternoon_out is None) and not vac and bool(exp.get('afternoon_out'))

            def show_time(v: Optional[str], default: str) -> str:
                if vac:
                    return default or ''
                if (row.mode or 'AUTO') == 'AUTO':
                    return v or (default or '')
                return v or ''

            mi = show_time(row.morning_in, exp['morning_in'])
            mo = show_time(row.morning_out, exp['morning_out'])
            ai = show_time(row.afternoon_in, exp['afternoon_in'])
            ao = show_time(row.afternoon_out, exp['afternoon_out'])
            mm = attendance_service.diff_minutes(mi, mo)
            ma = attendance_service.diff_minutes(ai, ao)
            cons_rows = AttendanceConsumption.query.filter_by(attendance_id=row.id).order_by(AttendanceConsumption.idx.asc()).all()
            calc = attendance_service.compute_work_minutes_and_flags(row)
            payable_time = attendance_service.fmt_minutes(calc['payable_min'])
            warn_text = ' | '.join(calc['warnings']) if calc['warnings'] else ''
            chip = ''
            chip_class = 'chip-rp'
            if vac:
                chip = 'VAC'
                chip_class = 'chip-vac'
            elif row.novelty == 'Razones particulares':
                chip = 'RP'
                chip_class = 'chip-rp'
            elif row.novelty == 'Curso':
                chip = 'Curso'
                chip_class = 'chip-curso'
            elif row.novelty == 'Delivery':
                chip = 'DEL'
                chip_class = 'chip-curso'
            novelty_ui = (row.novelty or '')
            if vac and novelty_ui != 'Vacaciones':
                novelty_ui = 'Vacaciones'
            rows.append({
                'employee': emp_name,
                'key': key,
                'group': g,
                'mode': (row.mode or 'AUTO'),
                'morning_in': mi,
                'morning_out': mo,
                'afternoon_in': ai,
                'afternoon_out': ao,
                'morning_time': attendance_service.fmt_minutes(mm),
                'afternoon_time': attendance_service.fmt_minutes(ma),
                'payable_time': payable_time,
                'novelty': novelty_ui,
                'novelty_minutes': int(row.novelty_minutes or 0),
                'notes': row.notes or '',
                'cons_count': len(cons_rows),
                'cons': cons_rows,
                'is_vac': bool(vac),
                'lock_times': bool(vac or (row.novelty == 'Delivery')),
                'has_warning': bool(calc['warnings']),
                'warn_text': warn_text,
                'chip': chip,
                'chip_class': chip_class,
                'mi_preset': mi_preset,
                'mo_preset': mo_preset,
                'ai_preset': ai_preset,
                'ao_preset': ao_preset,
            })

        if auto_fix_commit:
            db.session.commit()
        backup_caja_local_y_drive()
        summary = attendance_service.attendance_summary_rows(start_date, end_date, empf, novf)
        sum_payable_min = 0
        sum_cons = 0
        for rr in summary:
            try:
                hh, mm = str(rr.get('payable', '0:0')).split(':')
                sum_payable_min += int(hh) * 60 + int(mm)
            except Exception:
                pass
            sum_cons += int(rr.get('cons_total') or 0)
        sum_hours_txt = attendance_service.fmt_minutes(sum_payable_min)

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
