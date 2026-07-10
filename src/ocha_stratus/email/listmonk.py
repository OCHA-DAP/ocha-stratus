"""
Listmonk transactional email helpers.

Listmonk API: https://listmonk.app/docs/apis/transactional/

Delivery mechanics (from listmonk source, internal/messenger/email/email.go):
    Listmonk's /api/tx endpoint handles To, Cc, and Bcc headers differently:

    - To header:  COSMETIC ONLY. Sets the display header in the recipient's
      email client but does NOT add addresses to the SMTP envelope. To
      actually deliver to "To" recipients, they must be listed in
      subscriber_emails (with subscriber_mode="external").

    - Cc header:  DELIVERY + DISPLAY. Listmonk parses the Cc header, adds
      addresses to the SMTP envelope (em.Cc), and then removes the header
      (smtppool re-adds it). Recipients see Cc in their client AND receive
      the email.

    - Bcc header: DELIVERY ONLY. Same as Cc -- parsed into SMTP envelope
      (em.Bcc) and removed from headers. Recipients receive the email but
      are not visible to other recipients.

    Therefore, send_transactional() uses subscriber_emails for To recipients
    and relies on header parsing for Cc/Bcc delivery.
"""

import os

import requests

BASE_URL = (
    "https://listmonk-demo-afhcg8e2hde0fxca.eastus2-01.azurewebsites.net/api"
)

BASE_TRANSACTIONAL_ID = 12  # template set up for transactional emails.

USERNAME = os.getenv("DSCI_LISTMONK_API_USERNAME")
PASSWORD = os.getenv("DSCI_LISTMONK_API_KEY")


def send_transactional(
    to_emails: list[tuple[str, str]],
    subject: str,
    template_id: int = BASE_TRANSACTIONAL_ID,
    cc_emails: list[tuple[str, str]] = None,
    bcc_emails: list[tuple[str, str]] = None,
    data: dict = None,
):
    """Send a transactional email via Listmonk to multiple recipients.

    Parameters
    ----------
    to_emails : list of (name, email)
        Required. At least one recipient. Delivered via subscriber_emails
        (envelope). Displayed in the To header.
    subject : str
        Email subject line.
    template_id : int, optional
        Listmonk template ID to use. Defaults to BASE_TRANSACTIONAL_ID.
    cc_emails : list of (name, email), optional
        CC recipients. Delivered via Cc header (parsed into envelope by
        Listmonk). Visible to all recipients.
    bcc_emails : list of (name, email), optional
        BCC recipients. Delivered via Bcc header (parsed into envelope by
        Listmonk). Hidden from other recipients.
    data : dict, optional
        Template variables accessible as ``{{ .Tx.Data.* }}`` in the
        Listmonk template.

    Raises
    ------
    ValueError
        If to_emails is empty or not provided.
    """
    if not to_emails:
        raise ValueError("to_emails must contain at least one recipient")

    to_addresses = [email for _, email in to_emails]

    headers = [
        {"To": ", ".join(f"{name} <{email}>" for name, email in to_emails)}
    ]
    if cc_emails:
        headers.append(
            {"Cc": ", ".join(f"{name} <{email}>" for name, email in cc_emails)}
        )
    if bcc_emails:
        headers.append(
            {
                "Bcc": ", ".join(
                    f"{name} <{email}>" for name, email in bcc_emails
                )
            }
        )

    payload = {
        "subscriber_emails": to_addresses,
        "subscriber_mode": "external",
        "template_id": template_id,
        "from_email": "OCHA Data Science <ocha-datascience@un.org>",
        "content_type": "html",
        "subject": subject,
        "data": data or {},
        "headers": headers,
    }

    r = requests.post(
        f"{BASE_URL}/tx",
        auth=(USERNAME, PASSWORD),
        json=payload,
    )
    if not r.ok:
        print("Status:", r.status_code)
        print("Response text:", r.text)
        print("Payload sent:", payload)

    r.raise_for_status()
    return r.json()
