import os
import json

from .twilio_service import send_whatsapp_template
from .whatsapp_gateway_service import send_whatsapp_review_message
from . import repo


def process_pending(db):
    """
    Envía WhatsApps pendientes (due scheduled) usando:
    - número propio (whatsapp-web.js) si está activo y conectado
    - Twilio en caso contrario
    """
    print("########## PROCESS_PENDING ACTIVO ##########")

    pending = repo.get_due_scheduled(db)

    template_sid = (
        os.getenv("TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS")
        or os.getenv("TWILIO_CONTENT_SID_REVIEWS")
    )

    sent = 0
    failed = 0

    print(f"[send_due] encontrados: {len(pending)}")
    print(f"[send_due] template_sid={template_sid}")

    for rr in pending:
        try:
            review_url = repo.ensure_business_review_url(db, job_id=rr.job_id)
            settings = repo.get_business_settings(db, job_id=rr.job_id)

            name = (rr.customer_name or "").strip() or "😊"
            provider = getattr(settings, "whatsapp_provider", None) or "twilio"
            business_name = getattr(settings, "business_name", None) if settings else None
            personal_enabled = bool(
                getattr(settings, "whatsapp_personal_enabled", False)
            )
            session_status = getattr(settings, "whatsapp_session_status", None)

            print(
                f"[send_due] enviando rr={rr.id} "
                f"job_id={rr.job_id} "
                f"to={rr.phone_e164} "
                f"send_at={rr.send_at} "
                f"provider={provider} "
                f"session_status={session_status}"
            )

            if (
                provider == "personal_number"
                and personal_enabled
                and session_status == "ready"
            ):
                print(f"[send_due] usando numero propio rr={rr.id}")

                result = send_whatsapp_review_message(
                    job_id=rr.job_id,
                    phone_e164=rr.phone_e164,
                    customer_name=name,
                    business_name=business_name,
                    google_review_url=review_url,
                )

                print(
                    f"[send_due] personal send ok rr={rr.id} result={json.dumps(result, ensure_ascii=False)}"
                )

            else:
                if not template_sid:
                    raise RuntimeError(
                        "Missing WhatsApp template SID "
                        "(TWILIO_WHATSAPP_TEMPLATE_SID_REVIEWS / TWILIO_CONTENT_SID_REVIEWS)"
                    )

                variables = {
                    "1": name,
                    "2": review_url,
                }

                print(
                    f"[send_due] usando twilio rr={rr.id} "
                    f"vars={variables}"
                )

                sid = send_whatsapp_template(
                    to_e164=rr.phone_e164,
                    template_sid=template_sid,
                    variables=variables,
                )

                print(f"[send_due] twilio sid={sid} rr={rr.id}")

            repo.mark_sent(db, rr=rr)
            sent += 1

        except Exception as e:
            try:
                repo.mark_failed(db, rr=rr, error_message=str(e))
            except Exception as inner:
                db.rollback()
                print(f"[send_due] error marcando failed rr={rr.id}: {repr(inner)}")

            failed += 1
            print(f"[send_due] error enviando rr={rr.id}: {repr(e)}")

    return {
        "processed": len(pending),
        "sent": sent,
        "failed": failed,
    }