# utils/email_alert.py
import smtplib
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv

load_dotenv()

# Email config from environment variables
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD", "")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL", "")
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").lower() == "true"

def send_email(subject, body):
    """Simple email sender"""
    if not EMAIL_ENABLED:
        return
    
    if not SENDER_EMAIL or not SENDER_PASSWORD or not RECEIVER_EMAIL:
        print("⚠️ Email not configured - check .env file")
        return
    
    try:
        msg = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"] = SENDER_EMAIL
        msg["To"] = RECEIVER_EMAIL
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        print(f"📧 Email sent: {subject}")
        
    except Exception as e:
        print(f"❌ Email failed: {e}")

def notify(status, month, processed=0, failed=0, duration=None, error=None):
    """Simple notification function"""
    if status == "success":
        subject = f"✅ Pipeline Success - {month}"
        body = f"""
Pipeline: {month}
✅ Success: {processed} files
❌ Failed: {failed}
⏱️ Duration: {duration}
"""
    elif status == "failure":
        subject = f"❌ Pipeline Failed - {month}"
        body = f"""
Pipeline: {month}
❌ Error: {error}
"""
    else:
        return
    
    send_email(subject, body.strip())