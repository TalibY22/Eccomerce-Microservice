import asyncio
import os
import smtplib
from email.message import EmailMessage

import httpx


async def deliver(channel: str, destination: str, subject: str | None, body: str) -> str:
    mode = os.getenv(f"{channel.upper()}_PROVIDER", "log").lower()
    if mode == "log":
        print(f"notification channel={channel} destination={destination} subject={subject!r} body={body!r}", flush=True)
        return "logged"
    if channel == "email" and mode == "smtp":
        return await asyncio.to_thread(_smtp, destination, subject or "Notification", body)
    if channel in {"sms", "push"} and mode == "webhook":
        url = os.environ[f"{channel.upper()}_WEBHOOK_URL"]
        headers = {}
        token = os.getenv(f"{channel.upper()}_WEBHOOK_TOKEN")
        if token: headers["Authorization"] = f"Bearer {token}"
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(url, json={"to": destination, "subject": subject, "body": body}, headers=headers)
            response.raise_for_status()
            return str(response.headers.get("x-message-id", "accepted"))
    raise RuntimeError(f"unsupported provider {mode!r} for {channel}")


def _smtp(destination, subject, body):
    message = EmailMessage()
    message["From"] = os.environ["SMTP_FROM"]
    message["To"] = destination
    message["Subject"] = subject
    message.set_content(body)
    with smtplib.SMTP(os.environ["SMTP_HOST"], int(os.getenv("SMTP_PORT", "587")), timeout=10) as client:
        if os.getenv("SMTP_STARTTLS", "true").lower() == "true": client.starttls()
        user, password = os.getenv("SMTP_USER"), os.getenv("SMTP_PASSWORD")
        if user: client.login(user, password or "")
        client.send_message(message)
    return "accepted"
