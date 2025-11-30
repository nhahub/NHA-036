import os
import time
import traceback
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from io import StringIO
from azure.storage.blob import BlobServiceClient


AZURE_CONN_STR = "YOUR_KEY_HERE"
CONTAINER_NAME = "alertsoutput"

# Gmail config

GMAIL_USER = "yousef2006199@gmail.com"
APP_PASSWORD = "albw zzcp dvxw bumt"
RECEIVER = "yousef.abdelrahman.09@gmail.com"

# Local processed files

PROCESSED_FILE_PATH = r"D:\my_projects\Traffic_Alerts\processed_files.txt"

# Load processed files

processed_files = set()
if os.path.exists(PROCESSED_FILE_PATH):
    with open(PROCESSED_FILE_PATH, "r") as f:
        processed_files = set(line.strip() for line in f.readlines())
    print(f"📂 Loaded {len(processed_files)} previously processed files.\n")

# Connect to Azure

def connect_to_storage():
    try:
        client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
        print("✅ Successfully connected to Azure Storage.\n")
        return client
    except Exception as e:
        print("❌ Could not connect to Azure Storage:", e)
        return None

blob_service_client = connect_to_storage()
if not blob_service_client:
    exit()

container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Send email

def send_email(subject, body):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = GMAIL_USER
        msg["To"] = RECEIVER

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, APP_PASSWORD)
            server.send_message(msg)

        print(f"✉️ Email sent: {subject}")
    except Exception as e:
        print(f"❌ Failed to send email: {subject}")
        print(traceback.format_exc())

# Main loop

print("\n🚀 Traffic Alert System is now running...\n")

while True:
    try:
        for blob in container_client.list_blobs():
            if blob.name in processed_files: 
                continue  # Already processed

            print(f"📄 Detected new file: {blob.name}")

            # Download and decode blob
            blob_client = container_client.get_blob_client(blob)
            raw_data = blob_client.download_blob().readall()
            data_str = raw_data.decode("utf-8")
            df = pd.read_json(StringIO(data_str), lines=True)

            # Filter critical alerts
            critical_alerts = df[df["AlertLevel"] == 3] 

            if critical_alerts.empty:
                print(f"⚠️ No critical alerts found in {blob.name}\n")
            else:   
                print(f"🚨 {len(critical_alerts)} critical alert(s) found in {blob.name}")
                # Send email per row
                for index, row in critical_alerts.iterrows():
                    body = (
                        f"🚨 Critical Traffic Alert!\n\n"
                        f"📁 File: {blob.name}\n"
                        f"🕒 Event Time: {row['EventTime']} ⏰\n"
                        f"📍 Location: {row['location']} 🗺️\n"
                        f"🏎️ Average Speed: {row['avg_speed']} km/h ⚡\n"
                        f"⚠️ Alert Type: {row['AlertType']} 🚨\n"
                        f"🔴 Alert Level: {row['AlertLevel']} ❗\n"
                    )
                    subject = f"🚨 CRITICAL Traffic Alert - {blob.name} - Row {index}"
                    send_email(subject, body)

            # Save file locally
            local_folder = r"D:\my_projects\Traffic_Alerts\Downloaded_Files"
            os.makedirs(local_folder, exist_ok=True)
            local_file_path = os.path.join(local_folder, blob.name)
            with open(local_file_path, "w", encoding="utf-8") as f:
                f.write(data_str)

            # Mark file as processed
            processed_files.add(blob.name)
            with open(PROCESSED_FILE_PATH, "a") as f:
                f.write(blob.name + "\n")

            print(f"✅ File successfully processed and saved locally: {blob.name}\n")
            print("------------------------------------------------------------\n")

    except Exception as e:
        print("❌ ERROR in main loop:", e)
        print(traceback.format_exc())

    time.sleep(5)
