from datetime import date, datetime, timedelta
from flask import abort, redirect, render_template_string, request, session, url_for


def register_mobile_routes(
    app,
    *,
    db,
    login_required,
    admin_required,
    mobile_login_required,
    mobile_current_user,
    set_mobile_pin,
    check_mobile_pin,
    is_mobile_locked,
    pin_fingerprint,
    mobile_fail_state,
    mobile_set_lock,
    sync_advance_to_attendance,
    backup_caja_local_y_drive,
    safe_int,
    weekday_es,
    User,
    AdvanceRequest,
    BASE_CSS,
    ADMIN_PIN_HTML,
    M_LOGIN_HTML,
    M_MENU_HTML,
    M_ADV_NEW_HTML,
    M_ADV_ADMIN_HTML,
    M_ADV_HISTORY_HTML,
    MOBILE_SESSION_KEY,
):
    @app.route("/admin/pin_mobile", methods=["GET", "POST"])
    @login_required
    @admin_required
    def admin_pin_mobile():
        err = None
        msg = None
        if request.method == "POST":
            try:
                uid = int(request.form.get("uid") or 0)
                pin = (request.form.get("pin") or "").strip()
                u = db.session.get(User, uid)
                if not u:
                    raise ValueError("Usuario invalido.")
                set_mobile_pin(u, pin)
                db.session.commit()
                backup_caja_local_y_drive()
                msg = f"PIN actualizado para {u.username}."
            except Exception as ex:
                db.session.rollback()
                err = str(ex)

        users = User.query.filter_by(is_active=1).order_by(User.role.desc(), User.username.asc()).all()
        return render_template_string(ADMIN_PIN_HTML, base_css=BASE_CSS, users=users, err=err, msg=msg)

    @app.route("/m", methods=["GET", "POST"])
    def m_login():
        if mobile_current_user():
            return redirect(url_for("m_menu"))

        locked, until = mobile_fail_state()
        if locked:
            mins = int((until - datetime.utcnow()).total_seconds() // 60) + 1
            return render_template_string(M_LOGIN_HTML, err=f"Demasiados intentos. Espera {mins} min.", msg=None)

        err = None
        msg = None
        if request.method == "POST":
            pin = (request.form.get("pin") or "").strip()

            if not (pin.isdigit() and len(pin) == 4):
                err = "PIN invalido."
            else:
                fp = pin_fingerprint(pin)
                u = User.query.filter_by(mobile_pin_fingerprint=fp, is_active=1).first()

                if not u or not u.mobile_pin_hash:
                    fails = int(session.get("m_fail", 0)) + 1
                    session["m_fail"] = fails
                    if fails >= 5:
                        until = mobile_set_lock(10)
                        mins = int((until - datetime.utcnow()).total_seconds() // 60) + 1
                        err = f"Demasiados intentos. Espera {mins} min."
                    else:
                        err = "PIN incorrecto."
                else:
                    if is_mobile_locked(u):
                        mins = int((u.mobile_pin_locked_until - datetime.utcnow()).total_seconds() // 60) + 1
                        err = f"PIN bloqueado. Espera {mins} min."
                    elif not check_mobile_pin(u, pin):
                        u.mobile_pin_attempts = int(u.mobile_pin_attempts or 0) + 1
                        if u.mobile_pin_attempts >= 5:
                            u.mobile_pin_locked_until = datetime.utcnow() + timedelta(minutes=10)
                            u.mobile_pin_attempts = 0
                            db.session.commit()
                            backup_caja_local_y_drive()
                            err = "Demasiados intentos. Bloqueado 10 min."
                        else:
                            db.session.commit()
                            backup_caja_local_y_drive()
                            err = "PIN incorrecto."
                    else:
                        u.mobile_pin_attempts = 0
                        u.mobile_pin_locked_until = None
                        db.session.commit()
                        backup_caja_local_y_drive()

                        session[MOBILE_SESSION_KEY] = u.id
                        session.pop("m_fail", None)
                        session.pop("m_lock_until", None)
                        return redirect(url_for("m_menu"))

        return render_template_string(M_LOGIN_HTML, err=err, msg=msg)

    @app.route("/m/menu")
    @mobile_login_required
    def m_menu():
        return render_template_string(M_MENU_HTML, u=mobile_current_user())

    @app.route("/m/logout", methods=["POST"])
    def m_logout():
        session.pop(MOBILE_SESSION_KEY, None)
        return redirect(url_for("m_login"))

    @app.route("/m/adelantos/nuevo", methods=["GET", "POST"])
    @mobile_login_required
    def m_adv_new():
        u = mobile_current_user()
        err = None
        msg = None
        default_date = date.today().isoformat()

        if request.method == "POST":
            try:
                amount = safe_int(request.form.get("amount"))
                req_date_s = (request.form.get("req_date") or "").strip()
                reason = (request.form.get("reason") or "").strip() or None

                if amount is None or amount <= 0:
                    raise ValueError("Monto invalido.")
                if not req_date_s:
                    raise ValueError("Fecha invalida.")
                req_date = date.fromisoformat(req_date_s)

                ar = AdvanceRequest(
                    user_id=u.id,
                    amount_requested=int(amount),
                    requested_for_date=req_date,
                    reason=reason,
                    status="PENDING",
                    created_at=datetime.utcnow(),
                )
                db.session.add(ar)
                db.session.commit()
                backup_caja_local_y_drive()
                msg = "Solicitud enviada "
            except Exception as ex:
                db.session.rollback()
                err = str(ex)

        return render_template_string(M_ADV_NEW_HTML, err=err, msg=msg, default_date=default_date)

    @app.route("/m/admin/adelantos")
    @mobile_login_required
    def m_adv_admin():
        u = mobile_current_user()
        if not u or u.role != "admin":
            abort(403)

        rows = (
            db.session.query(AdvanceRequest, User)
            .join(User, User.id == AdvanceRequest.user_id)
            .filter(AdvanceRequest.status == "PENDING")
            .order_by(AdvanceRequest.created_at.asc())
            .all()
        )
        return render_template_string(M_ADV_ADMIN_HTML, rows=rows, err=None, msg=None)

    @app.route("/m/admin/adelantos/<int:adv_id>/decide", methods=["POST"])
    @mobile_login_required
    def m_adv_decide(adv_id: int):
        u = mobile_current_user()
        if not u or u.role != "admin":
            abort(403)

        decision = (request.form.get("decision") or "").strip().upper()
        admin_comment = (request.form.get("admin_comment") or "").strip() or None
        if decision not in ("APPROVE", "REJECT"):
            return redirect(url_for("m_adv_admin"))

        ar = db.session.get(AdvanceRequest, adv_id)
        if not ar:
            abort(404)
        if ar.status != "PENDING":
            return redirect(url_for("m_adv_admin"))

        ar.status = "APPROVED" if decision == "APPROVE" else "REJECTED"
        ar.decided_at = datetime.utcnow()
        ar.decided_by_user_id = u.id
        ar.admin_comment = admin_comment
        if ar.status == "APPROVED":
            sync_advance_to_attendance(ar)
        db.session.commit()
        backup_caja_local_y_drive()
        return redirect(url_for("m_adv_admin"))

    @app.route("/m/adelantos/historial")
    @mobile_login_required
    def m_adv_history():
        u = mobile_current_user()
        if not u:
            abort(403)

        q = db.session.query(AdvanceRequest, User).join(User, User.id == AdvanceRequest.user_id)
        if u.role != "admin":
            q = q.filter(AdvanceRequest.user_id == u.id)

        rows_db = q.order_by(AdvanceRequest.created_at.desc()).limit(200).all()

        rows = []
        for ar, usr in rows_db:
            req_d = ar.requested_for_date or ar.created_at.date()
            rows.append(
                {
                    "employee": (getattr(usr, "username", None) or getattr(usr, "name", None) or ""),
                    "weekday": weekday_es(req_d),
                    "req_date": req_d.isoformat(),
                    "amount": int(ar.amount_requested or 0),
                    "status": ar.status or "PENDING",
                    "comment": (ar.admin_comment or None),
                }
            )

        return render_template_string(M_ADV_HISTORY_HTML, rows=rows, is_admin=(u.role == "admin"))
