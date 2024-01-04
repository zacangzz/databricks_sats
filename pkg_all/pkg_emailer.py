__author__ = "Zac Ang"
__email__ = "zhaozong.ang@gmail.com"
"""
pkg_emailer.py
Emailer modules designed to help with sending out email notifications
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from email.utils import COMMASPACE

import os
from dotenv import load_dotenv

load_dotenv()

def attach_file(message, file_path):
    with open(file_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        f"attachment; filename={file_path}",
    )
    message.attach(part)

def create_message(sender, receivers, body_text, cc=None, bcc=None, subject="", MIME_type="html"):
    message = MIMEMultipart()
    message["From"] = sender
    message["To"] = COMMASPACE.join(receivers)
    if cc:
        message["Cc"] = COMMASPACE.join(cc)
    if bcc:
        message["Bcc"] = COMMASPACE.join(bcc)
    message["Subject"] = subject

    body = MIMEText(body_text, MIME_type)
    message.attach(body)

    # Check for None before concatenating
    all_recipients = receivers
    if cc is not None:
        all_recipients += cc
    if bcc is not None:
        all_recipients += bcc

    return message, all_recipients

def send_email(sender, receivers, message_text):
    with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
        server.starttls()
        server.login(sender, os.getenv('HC_ANALYTICS_PW'))
        server.sendmail(sender, receivers, message_text)
    
###