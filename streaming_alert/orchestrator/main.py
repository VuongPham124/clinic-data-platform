import base64
import hashlib
import json
import os
import smtplib
import urllib.request
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

from flask import Flask, request
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token as google_id_token
from google.cloud import bigquery
from google.cloud import firestore

app = Flask(__name__)

PROJECT_ID = os.getenv("PROJECT_ID", "wata-clinicdataplatform-gcp")
FIRESTORE_COLLECTION = os.getenv("FIRESTORE_COLLECTION", "alert_state")
BQ_TABLE_FQN = os.getenv("BQ_TABLE_FQN", f"{PROJECT_ID}.ops_monitor.alert_history")
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "900"))
NOTIFY_WEBHOOK_URL = os.getenv("NOTIFY_WEBHOOK_URL", "")
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_PROVIDER = os.getenv("EMAIL_PROVIDER", "smtp").lower()
EMAIL_FROM = os.getenv("EMAIL_FROM", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
PUBSUB_AUTH_REQUIRED = os.getenv("PUBSUB_AUTH_REQUIRED", "true").lower() == "true"
PUBSUB_AUTH_AUDIENCE = os.getenv("PUBSUB_AUTH_AUDIENCE", "")
PUBSUB_ALLOWED_SERVICE_ACCOUNTS = {
    x.strip().lower() for x in os.getenv("PUBSUB_ALLOWED_SERVICE_ACCOUNTS", "").split(",") if x.strip()
}

_db = firestore.Client(project=PROJECT_ID)
_bq = bigquery.Client(project=PROJECT_ID)
_token_request = google_auth_requests.Request()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _parse_iso(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _alert_doc_id(alert_key: str) -> str:
    return hashlib.sha1(alert_key.encode("utf-8")).hexdigest()


def _send_notification(payload: dict):
    if not NOTIFY_WEBHOOK_URL:
        pass
    else:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            NOTIFY_WEBHOOK_URL,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)

    if EMAIL_ENABLED:
        _send_email(payload)


def _send_email(payload: dict):
    if EMAIL_PROVIDER == "smtp":
        _send_email_smtp(payload)
        return
    if EMAIL_PROVIDER != "sendgrid":
        print("Unsupported EMAIL_PROVIDER; skip email")
        return
    if not SENDGRID_API_KEY or not EMAIL_FROM or not EMAIL_TO:
        print("Email config missing; skip email")
        return

    recipients = [x.strip() for x in EMAIL_TO.split(",") if x.strip()]
    if not recipients:
        print("EMAIL_TO empty; skip email")
        return

    subject = f"[{payload.get('payload', {}).get('severity', 'unknown')}] {payload.get('payload', {}).get('alert_type', 'alert')}"
    content = json.dumps(payload, indent=2, ensure_ascii=False)
    body = {
        "personalizations": [{"to": [{"email": r} for r in recipients]}],
        "from": {"email": EMAIL_FROM},
        "subject": subject,
        "content": [{"type": "text/plain", "value": content}],
    }

    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
        },
        method="POST",
    )
    urllib.request.urlopen(req, timeout=10)


def _send_email_smtp(payload: dict):
    if not EMAIL_FROM or not EMAIL_TO or not SMTP_USER or not SMTP_PASSWORD:
        print("SMTP email config missing; skip email")
        return

    recipients = [x.strip() for x in EMAIL_TO.split(",") if x.strip()]
    if not recipients:
        print("EMAIL_TO empty; skip email")
        return

    subject = f"[{payload.get('payload', {}).get('severity', 'unknown')}] {payload.get('payload', {}).get('alert_type', 'alert')}"
    content = json.dumps(payload, indent=2, ensure_ascii=False)

    msg = EmailMessage()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(content)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASSWORD)
        smtp.send_message(msg)


def _verify_pubsub_auth(req) -> bool:
    if not PUBSUB_AUTH_REQUIRED:
        return True

    # Prefer Cloud Run authenticated identity header (stable for IAM-invoked requests).
    authenticated_email = (req.headers.get("X-Goog-Authenticated-User-Email") or "").strip().lower()
    if authenticated_email:
        # Header format is typically "accounts.google.com:<email>".
        if ":" in authenticated_email:
            authenticated_email = authenticated_email.split(":", 1)[1].strip()
        if not PUBSUB_ALLOWED_SERVICE_ACCOUNTS:
            return True
        return authenticated_email in PUBSUB_ALLOWED_SERVICE_ACCOUNTS

    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False

    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        return False

    audience = PUBSUB_AUTH_AUDIENCE or req.base_url
    try:
        claims = google_id_token.verify_oauth2_token(token, _token_request, audience=audience)
    except Exception:
        return False

    issuer = claims.get("iss")
    if issuer not in ("accounts.google.com", "https://accounts.google.com"):
        return False

    if PUBSUB_ALLOWED_SERVICE_ACCOUNTS:
        email = (claims.get("email") or "").lower()
        # Some Google-issued tokens may not carry the email claim.
        # In that case, we already rely on Cloud Run IAM auth at ingress.
        if email and email not in PUBSUB_ALLOWED_SERVICE_ACCOUNTS:
            return False

    return True


def _insert_history(alert_id: str, alert_key: str, payload: dict, emitted: bool):
    row = {
        "alert_id": alert_id,
        "alert_key": alert_key,
        "alert_type": payload.get("alert_type", "unknown"),
        "severity": payload.get("severity", "unknown"),
        "source_table": payload.get("source_table", "unknown"),
        "event_time": payload.get("event_time"),
        "payload_json": json.dumps(payload, separators=(",", ":")),
        "created_at": _iso(_now()),
        "emitted": emitted,
    }
    errors = _bq.insert_rows_json(BQ_TABLE_FQN, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")


@firestore.transactional
def _apply_alert_state_transaction(transaction, doc_ref, payload: dict, now: datetime):
    snap = doc_ref.get(transaction=transaction)
    old = snap.to_dict() if snap.exists else {}

    last_sent_at = _parse_iso(old.get("last_sent_at"))
    cooldown_until = last_sent_at + timedelta(seconds=COOLDOWN_SECONDS) if last_sent_at else None
    in_cooldown = bool(cooldown_until and now < cooldown_until)
    emitted = not in_cooldown

    new_state = {
        "alert_key": payload.get("alert_key"),
        "alert_type": payload.get("alert_type", "unknown"),
        "severity": payload.get("severity", "unknown"),
        "source_table": payload.get("source_table", "unknown"),
        "last_event_time": payload.get("event_time"),
        "last_seen_at": _iso(now),
        "last_payload": payload,
        "cooldown_seconds": COOLDOWN_SECONDS,
    }
    if emitted:
        new_state["last_sent_at"] = _iso(now)

    transaction.set(doc_ref, new_state, merge=True)
    return emitted, new_state


def _process_alert(payload: dict):
    alert_key = payload.get("alert_key") or f"{payload.get('alert_type', 'unknown')}|{payload.get('source_table', 'unknown')}"
    payload["alert_key"] = alert_key
    now = _now()
    doc_id = _alert_doc_id(alert_key)
    doc_ref = _db.collection(FIRESTORE_COLLECTION).document(doc_id)
    transaction = _db.transaction()
    emitted, new_state = _apply_alert_state_transaction(transaction, doc_ref, payload, now)

    if emitted:
        _send_notification(
            {
                "text": f"[{new_state['severity']}] {new_state['alert_type']} - {new_state['source_table']}",
                "payload": payload,
            }
        )

    _insert_history(alert_id=doc_id, alert_key=alert_key, payload=payload, emitted=emitted)
    return emitted, doc_id


@app.post("/pubsub/push")
def pubsub_push():
    if not _verify_pubsub_auth(request):
        return ("unauthorized", 401)

    body = request.get_json(silent=True) or {}
    msg = (((body.get("message") or {}).get("data")) or "")
    if not msg:
        return ("", 204)

    payload = json.loads(base64.b64decode(msg).decode("utf-8"))
    emitted, alert_id = _process_alert(payload)
    print(
        json.dumps(
            {
                "received_at": _iso(_now()),
                "alert_id": alert_id,
                "alert_key": payload.get("alert_key"),
                "emitted": emitted,
            }
        )
    )
    return ("", 204)


@app.get("/healthz")
def healthz():
    return {"ok": True}
