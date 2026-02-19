import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def _as_bool(v: str | None) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def send_email_smtp(to_email: str, subject: str, body_text: str) -> None:
    # Lê primeiro EMAIL_*, e se não existir, cai para SMTP_* (compatibilidade)
    host = (os.getenv("EMAIL_HOST") or os.getenv("SMTP_HOST") or "").strip()
    port = int((os.getenv("EMAIL_PORT") or os.getenv("SMTP_PORT") or "587").strip())

    user = (os.getenv("EMAIL_USERNAME") or os.getenv("SMTP_USER") or "").strip()
    password = (os.getenv("EMAIL_PASSWORD") or os.getenv("SMTP_PASS") or "").strip()

    from_email = (os.getenv("EMAIL_SENDER") or os.getenv("SMTP_FROM") or user).strip()

    use_tls = _as_bool(os.getenv("EMAIL_USE_TLS")) or _as_bool(os.getenv("SMTP_USE_TLS"))
    use_ssl = _as_bool(os.getenv("EMAIL_USE_SSL")) or _as_bool(os.getenv("SMTP_USE_SSL"))

    if not host or not user or not password or not from_email:
        raise RuntimeError(
            "SMTP não configurado (EMAIL_HOST/EMAIL_PORT/EMAIL_USERNAME/EMAIL_PASSWORD/EMAIL_SENDER)."
        )

    msg = MIMEMultipart()
    msg["From"] = 'ZionDocs <tscdev2@gmail.com>'
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    timeout = 20

    # SSL direto (normalmente 465)
    if use_ssl or port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=timeout) as server:
            server.login(user, password)
            server.sendmail(from_email, [to_email], msg.as_string())
        return

    # SMTP normal + opcional STARTTLS (normalmente 587)
    with smtplib.SMTP(host, port, timeout=timeout) as server:
        server.ehlo()
        if use_tls:
            server.starttls()
            server.ehlo()
        server.login(user, password)
        server.sendmail(from_email, [to_email], msg.as_string())
