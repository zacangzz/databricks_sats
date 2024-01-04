# Databricks notebook source
from pkg_all import em
from datetime import datetime
import json
import os
from dotenv import load_dotenv

# COMMAND ----------

load_dotenv()

# COMMAND ----------

def send_rec_status():
    attachment_path = "data/errorlist.xlsx"
    sender = os.environ['R_EMAIL_SENDER']
    receiver_email = json.loads(os.environ['R_EMAIL_RECEIVERS'])

    with open('data/email_body.html', 'r') as f:
        email_body = f.read()
    formatted_date = datetime.now().strftime("%d %B %Y")
    subject = f"Recruitment Status - Missing or Invalid Fields in Recruitment Tracker {formatted_date}"

    message, all_recipients = em.create_message(sender, receiver_email, email_body, subject=subject)
    em.attach_file(message, attachment_path)
    em.send_email(sender, all_recipients, message.as_string())
send_rec_status()
