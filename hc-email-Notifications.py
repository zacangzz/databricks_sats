# Databricks notebook source
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# load confi secrets from .env
import os
from dotenv import load_dotenv
load_dotenv()

# COMMAND ----------

def create_message(sender, receiver, subject, body_text):
    message = MIMEMultipart()
    message["From"] = sender
    message["To"] = receiver
    message["Subject"] = subject

    body = MIMEText(body_text)
    message.attach(body)

    return message

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

def send_email(sender, receiver, message_text):
    with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
        server.starttls()
        server.login(sender, os.getenv('HC_ANALYTICS_PW'))
        server.sendmail(sender, receiver, message_text)

# COMMAND ----------

attachment_path = "/dbfs/dbfs/install_msodbcsql17.sh"
sender_email = "hc_analytics@sats.com.sg"
receiver_email = "zac_angzz@sats.com.sg"
email_body = 'Test Message'

message = create_message(sender_email, receiver_email, "Test Message", email_body)
attach_file(message, attachment_path)
send_email(sender_email, receiver_email, message.as_string())


# COMMAND ----------


