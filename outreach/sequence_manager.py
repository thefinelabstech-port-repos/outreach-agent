from pathlib import Path
from core.models import Lead
from outreach.email_sender import send_email


TEMPLATE_DIR = Path(__file__).parent / "templates"


def load_template(name: str) -> str:
    """Load an email template by filename from the templates/ directory."""
    path = TEMPLATE_DIR / name
    return path.read_text(encoding="utf-8")


def render_template(template: str, lead: Lead) -> str:
    """Replace placeholders in a template with lead data."""
    return (
        template
        .replace("{{company_name}}", lead.name)
        .replace("{{website}}", lead.website or "")
        .replace("{{industry}}", lead.industry or "")
        .replace("{{location}}", lead.location or "")
    )


def run_sequence(lead: Lead, template_name: str = "cold_email.txt") -> bool:
    """
    Send the first cold email in the sequence to a lead.
    Returns True if sent successfully.
    """
    if not lead.emails:
        print(f"[Sequence] No email for {lead.name}, skipping.")
        return False

    template = load_template(template_name)
    body     = render_template(template, lead)
    subject  = f"Quick question for {lead.name}"
    to       = lead.emails.split(",")[0].strip()

    success = send_email(to, subject, body)
    if success:
        print(f"[Sequence] ✓ Sent to {to} ({lead.name})")
    return success
