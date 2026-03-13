import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


BREVO_SMTP_HOST = "smtp-relay.brevo.com"
BREVO_SMTP_PORT = 587
BREVO_SMTP_KEY  = os.getenv("BREVO_SMTP_KEY", "")
SENDER_EMAIL    = os.getenv("SENDER_EMAIL", "")


def send_email(to: str, subject: str, body: str) -> bool:
    """Send a plain-text email via Brevo SMTP. Returns True on success."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = to
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(BREVO_SMTP_HOST, BREVO_SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SENDER_EMAIL, BREVO_SMTP_KEY)
            server.sendmail(SENDER_EMAIL, to, msg.as_string())
        return True
    except Exception as e:
        print(f"[EmailSender] Failed to send to {to}: {e}")
        return False
