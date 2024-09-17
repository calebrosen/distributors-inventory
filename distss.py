import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QPushButton, QLabel, QVBoxLayout, QWidget, QCheckBox, QHBoxLayout
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QObject
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
import logging
from pynput.keyboard import Key, Controller as KeyboardController
from pynput.mouse import Button, Controller as MouseController
import webbrowser
import os
import io
import time
import threading
import smtplib
import openpyxl
from email.mime.text import MIMEText
import re
from email.mime.multipart import MIMEMultipart
import requests
import csv
import json
from urllib.parse import quote, urlencode
import requests
import pandas as pd
from requests.exceptions import RequestException
from datetime import datetime
from dotenv.main import load_dotenv
load_dotenv()


# ██╗   ██╗ █████╗ ██████╗ ██╗ █████╗ ██████╗ ██╗     ███████╗███████╗
# ██║   ██║██╔══██╗██╔══██╗██║██╔══██╗██╔══██╗██║     ██╔════╝██╔════╝
# ██║   ██║███████║██████╔╝██║███████║██████╔╝██║     █████╗  ███████╗
# ╚██╗ ██╔╝██╔══██║██╔══██╗██║██╔══██║██╔══██╗██║     ██╔══╝  ╚════██║
#  ╚████╔╝ ██║  ██║██║  ██║██║██║  ██║██████╔╝███████╗███████╗███████║
#   ╚═══╝  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚══════╝╚══════╝

# email for error catching
smtp_server = os.getenv("SMTP_SERVER")
smtp_port = int(os.getenv("SMTP_PORT"))
gmail_user = os.getenv("GMAIL_USER")
gmail_password = os.getenv("GMAIL_PASSWORD")
it_email = os.getenv("RECEIVER_EMAIL")

# zoho stuff
zoho_client_id = os.getenv("ZOHO_CLIENT_ID")
zoho_client_secret = os.getenv("ZOHO_CLIENT_SECRET")
zoho_refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
zoho_mail_account_id = os.getenv("ZOHO_MAIL_ACCOUNT_ID")
zoho_mail_folder_id = os.getenv("ZOHO_MAIL_FOLDER_ID")

# other
log_messages = []
current_date = datetime.now().strftime("%Y%m%d")
current_date_w_dashes = datetime.now().strftime("%Y-%m-%d")
csv_folder_path = os.getenv("CSV_FOLDER_PATH")

# ███████╗███╗   ██╗██████╗     ██╗   ██╗ █████╗ ██████╗ ██╗ █████╗ ██████╗ ██╗     ███████╗███████╗
# ██╔════╝████╗  ██║██╔══██╗    ██║   ██║██╔══██╗██╔══██╗██║██╔══██╗██╔══██╗██║     ██╔════╝██╔════╝
# █████╗  ██╔██╗ ██║██║  ██║    ██║   ██║███████║██████╔╝██║███████║██████╔╝██║     █████╗  ███████╗
# ██╔══╝  ██║╚██╗██║██║  ██║    ╚██╗ ██╔╝██╔══██║██╔══██╗██║██╔══██║██╔══██╗██║     ██╔══╝  ╚════██║
# ███████╗██║ ╚████║██████╔╝     ╚████╔╝ ██║  ██║██║  ██║██║██║  ██║██████╔╝███████╗███████╗███████║
# ╚══════╝╚═╝  ╚═══╝╚═════╝       ╚═══╝  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚══════╝╚══════╝
                                                                                                                                                                                                                                                                             
def send_error_email(error_message):
    sender_email = gmail_user
    receiver_email = it_email
    subject = "Inventory Spreadsheet Python Erorr"
    body = f"An error occurred when running inventory spreadsheets python code:\n\n{error_message}"

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(gmail_user, gmail_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print("Error sending email:", e)
    finally:
        server.quit()
    
def append_log_messages(log_message):
    global log_messages
    log_messages.append(log_message)
    print(f"Log appended: {log_message}")

class WorkerSignals(QObject):
    update_log_display = pyqtSignal()
    
class MainWindow(QMainWindow):
    
    def __init__(self):
        super().__init__()
        global main_window_instance
        main_window_instance = self
        self.signals = WorkerSignals()
        self.signals.update_log_display.connect(self.update_log_display)
        self.access_token = None
        self.setWindowTitle("IRG Distributor Spreadsheets")
        self.setGeometry(100, 100, 600, 500)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        
        self.label = QLabel("", self)
        self.label.setStyleSheet("background-color: black; color: white; font-family: 'Courier'; font-size: 14px;")
        self.label.setAlignment(Qt.AlignmentFlag.AlignTop)
        main_layout.addWidget(self.label)

        button_layout = QHBoxLayout()
        self.downloadSelectedButton = QPushButton("Download Selected", self)
        self.downloadSelectedAndUploadToCreatorButton = QPushButton("Downloaded Selected and Upload to Creator", self)
        self.uploadToCreatorButton = QPushButton("Upload to Creator", self)
        button_layout.addWidget(self.downloadSelectedButton)
        button_layout.addWidget(self.downloadSelectedAndUploadToCreatorButton)
        button_layout.addWidget(self.uploadToCreatorButton)
        main_layout.addLayout(button_layout)

        checkbox_layout = QHBoxLayout()
        self.checkboxes = []
        checkbox_labels = ["AES", "AZF", "FOR", "RMI", "RUT", "TSD", "PIN", "PCS"]
        selected_distribors = checkbox_labels
        for label in checkbox_labels:
            checkbox = QCheckBox(label, self)
            checkbox.setChecked(True)
            checkbox.stateChanged.connect(self.update_selected_distributors)
            checkbox_layout.addWidget(checkbox)
            self.checkboxes.append(checkbox)

        main_layout.addLayout(checkbox_layout)

        self.downloadSelectedButton.clicked.connect(self.downloadSelected)
        self.downloadSelectedAndUploadToCreatorButton.clicked.connect(self.downloadSelectedAndUploadToCreator)
        self.uploadToCreatorButton.clicked.connect(self.uploadToCreator)

        self.setStyleSheet("""
            * {
                font-family: 'Arial';
                font-size: 18px;
                font-weight: bold;
            }

            QMainWindow {
                background-color: #4d4d4d;
            }

            QLabel {
                background-color: #fff;
                padding: 10px;
                border: 1px solid red;
                border-radius: 5px;
                color: #000;
                font-size: 17px;
            }

            QPushButton {
                background-color: #000;
                border-radius: 6px;
                padding: 10px;
                color: #fff;
            }

            QPushButton:hover {
                background-color: #292828;
            }

            QCheckBox {
                font-size: 16px;
                padding: 10px;
            }
            QCheckBox::indicator {
                border: 2px solid #fff;
                border-radius: 6px;
                width: 20px;
                height: 20px;
            }

            QCheckBox::indicator:checked {
                background-color: #ff0000;
                border: 2px solid #000;
            }
        """)
        
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_log_display)
        self.timer.start(1000)
        
        self.update_selected_distributors()
        
    def update_log_display(self):
        log_display = "<br>".join(log_messages)
        self.label.setText(f"<pre>{log_display}</pre>")
        
    def update_selected_distributors(self):
        global selected_distributors
        selected_distributors = []
        for checkbox in self.checkboxes:
            if checkbox.isChecked():
                selected_distributors.append(checkbox.text())
        print(f"Selected distributors: {selected_distributors}")
        append_log_messages(f"Selected distributors: {selected_distributors}")
 
    def downloadSelected(self):
        self.getAccessTokenFromRefreshToken()
        
        def update_log_display_thread():
            self.signals.update_log_display.emit()
        
        def process_distributor(dist):
            update_thread = threading.Thread(target=update_log_display_thread)
            update_thread.start()
            func_name = f"get_{dist.lower()}_spreadsheet"
            func = globals().get(func_name)
            if callable(func):
                func(self.access_token)
            else:
                print(f"Function {func_name} not found")
        
        threads = []
        for dist in selected_distributors:
            thread = threading.Thread(target=process_distributor, args=(dist,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()

    def downloadSelectedAndUploadToCreator(self):
        append_log_messages("Download selected and uploaded to creator was clicked!")

    def uploadToCreator(self):
        append_log_messages("Upload to creator was clicked!")

    def getAccessTokenFromRefreshToken(self):
        try:
            append_log_messages("Retrieving token...")
            url = f"https://accounts.zoho.com/oauth/v2/token"
            payload = {
                "refresh_token": zoho_refresh_token,
                "client_id": zoho_client_id,
                "client_secret": zoho_client_secret,
                "grant_type": "refresh_token"
            }
            response = requests.post(url, data=payload)
            
            if response.status_code == 200:
                data = response.json()
                access_token = data.get("access_token")
                self.access_token = access_token
                append_log_messages(f"Access token: {access_token}")
                
        except Exception as e:
            print("Error getting access token:", e)
            append_log_messages(f"Error getting access token: {e}")
            send_error_email("Error getting access token")


#      ___       _______     _______.
#     /   \     |   ____|   /       |
#    /  ^  \    |  |__     |   (----`
#   /  /_\  \   |   __|     \   \
#  /  _____  \  |  |____.----)   |
# /__/     \__\ |_______|_______/
                                   

def get_aes_spreadsheet(access_token):
    aes_search_param = urllib.parse.quote_plus("INVENTORY REPORT")
    aes_email_from = urllib.parse.quote_plus("dustinp@aes4home.com")
    aes_message_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/messages/search?"
                       f"searchKey=subject:{aes_search_param}::sender:{aes_email_from}")
    
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }
        
    try:
        response = requests.get(aes_message_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"AES Message request failed: {e}")
        return

    response_data = response.json()

    aes_message_id = None
    aes_email_received_date = None
    
    if 'data' in response_data:
        aes_data = response_data['data']
        if isinstance(aes_data, list) and len(aes_data) > 0:
            first_element = aes_data[0]
            if 'messageId' in first_element:
                aes_message_id = first_element['messageId']
                append_log_messages(f'AES Message ID: {aes_message_id}')
                aes_email_received_date_unix_epoch = first_element.get('receivedTime')
                if aes_email_received_date_unix_epoch:
                    aes_email_received_date = datetime.utcfromtimestamp(int(aes_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
    if not aes_message_id:
        append_log_messages("No AES messages found.")
        return
    
    aes_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{aes_message_id}/attachmentinfo")
    
    response = requests.get(aes_attachment_info_url, headers=headers)
    response_data = response.json()
    
    aes_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        aes_attachments = response_data['data']['attachments']
        for attachment in aes_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                aes_attachment_id = attachment['attachmentId']
                append_log_messages(f'AES Attachment ID: {aes_attachment_id}')
                break
    
    if not aes_attachment_id:
        append_log_messages("No AES CSV attachment found.")
        return
    
    aes_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{aes_message_id}/attachments/{aes_attachment_id}")
    
    response = requests.get(aes_attachment_download_url, headers=headers)
    
    csv_file_aes = os.path.join(csv_folder_path, 'aes.csv')
    
    def parse_csv_with_line_breaks(data, num_columns):
        rows = []
        current_row = []
        current_field = ''
        within_quotes = False
        field_count = 0
        
        data_length = len(data)
        i = 0
        
        while i < data_length:
            char = data[i]
            
            if char == '"':
                if within_quotes and i + 1 < data_length and data[i + 1] == '"':
                    current_field += '"'
                    i += 1
                else:
                    within_quotes = not within_quotes
            elif char == ',' and not within_quotes:
                current_row.append(current_field)
                current_field = ''
                field_count += 1
                
                if field_count == num_columns:
                    if i + 1 < data_length and (data[i + 1] == "\n" or data[i + 1] == "\r"):
                        i += 1
                        while i + 1 < data_length and (data[i + 1] == "\n" or data[i + 1] == "\r"):
                            i += 1
                    rows.append(current_row)
                    current_row = []
                    field_count = 0
            elif (char == "\n" or char == "\r") and not within_quotes:
                current_row.append(current_field)
                current_field = ''
                field_count += 1
                
                if field_count == num_columns:
                    rows.append(current_row)
                    current_row = []
                    field_count = 0
                while i + 1 < data_length and (data[i + 1] == "\n" or data[i + 1] == "\r"):
                    i += 1
            else:
                current_field += char
            
            i += 1
        
        if current_field or current_row:
            current_row.append(current_field)
            rows.append(current_row)
        
        return rows
    
    csv_data_aes = parse_csv_with_line_breaks(response.content.decode('utf-8'), 15)
    
    with open(csv_file_aes, 'w', newline='', encoding="utf-8") as fp_aes:
        writer = csv.writer(fp_aes)
        row_count = 0
        for fields_aes in csv_data_aes:
            if len(fields_aes) == 15:
                writer.writerow(fields_aes)
                row_count += 1
    
    if row_count > 0:
        append_log_messages(f"AES Date Received: {aes_email_received_date}")
        append_log_messages(f"AES Rows: {row_count}")
    else:
        append_log_messages(f"No data was written to {csv_file_aes}")


#  _______ .__   __.  _______          ___       _______     _______.
# |   ____||  \ |  | |       \        /   \     |   ____|   /       |
# |  |__   |   \|  | |  .--.  |      /  ^  \    |  |__     |   (----`
# |   __|  |  . `  | |  |  |  |     /  /_\  \   |   __|     \   \
# |  |____ |  |\   | |  '--'  |    /  _____  \  |  |____.----)   |
# |_______||__| \__| |_______/    /__/     \__\ |_______|_______/
   
                                                                   
# ---------------------------------------------------------------------------------


#      ___      ________   _______
#     /   \    |       /  |   ____|
#    /  ^  \   `---/  /   |  |__
#   /  /_\  \     /  /    |   __|
#  /  _____  \   /  /----.|  |
# /__/     \__\ /________||__|


def get_azf_spreadsheet(access_token):
    azf_search_param = urllib.parse.quote_plus("AF Distributors - Inventory and Pricing Report")
    azf_file_name = urllib.parse.quote_plus("Inventory + Pricing Report.csv")
    azf_message_url = f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/messages/search?searchKey=subject:{azf_search_param}::fileName:{azf_file_name}"
    
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(azf_message_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"AZF Message request failed: {e}")
        return

    response_data = response.json()

    azf_message_id = None
    azf_email_received_date = None
    
    if 'data' in response_data:
        azf_data = response_data['data']
        if isinstance(azf_data, list) and len(azf_data) > 0:
            first_element = azf_data[0]
            if 'messageId' in first_element:
                azf_message_id = first_element['messageId']
                append_log_messages(f'AZF Message ID: {azf_message_id}')
                azf_email_received_date_unix_epoch = first_element.get('receivedTime')
                if azf_email_received_date_unix_epoch:
                    azf_email_received_date = datetime.utcfromtimestamp(int(azf_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
    if not azf_message_id:
        append_log_messages("No AZF message found.")
        return
    
    azf_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{azf_message_id}/attachmentinfo")
    
    try:
        response = requests.get(azf_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"AZF Attachment Info failed: {e}")
        return

    response_data = response.json()
    
    azf_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        azf_attachments = response_data['data']['attachments']
        for attachment in azf_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                azf_attachment_id = attachment['attachmentId']
                append_log_messages(f'AZF Attachment ID: {azf_attachment_id}')
                break
    
    if not azf_attachment_id:
        append_log_messages("No AZF CSV attachment found.")
        return
    
    azf_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{azf_message_id}/attachments/{azf_attachment_id}")
    
    try:
        response = requests.get(azf_attachment_download_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"AZF attachment download failed: {e}")
        return

    response_azf_download = response.content.decode('utf-16')

    csv_file_azf = os.path.join(csv_folder_path, 'azf.csv')

    def get_csv_fields_azf(line):
        return list(csv.reader(io.StringIO(line), delimiter='\t' if '\t' in line else ','))[0]

    def process_azf_data(response_azf_download):
        azf_rows = [line for line in response_azf_download.splitlines() if line.strip()]
        
        if len(azf_rows) > 1:
            headers_azf = get_csv_fields_azf(azf_rows.pop(0))
            if len(azf_rows) > 1:
                azf_rows.pop(1)
                
            csv_data_azf = [get_csv_fields_azf(row) for row in azf_rows]
            filtered_csv_data_azf = [row if any(field.strip() for field in row) else None for row in csv_data_azf]
            
            with open(csv_file_azf, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                row_count = 0

                if headers_azf:
                    writer.writerow(headers_azf)
                    row_count += 1
                
                for fields_azf in filtered_csv_data_azf:
                    if fields_azf:
                        writer.writerow(fields_azf)
                        row_count += 1
                
            append_log_messages(f'AZF Date Received: {azf_email_received_date}')
            append_log_messages(f'AZF Rows: {row_count}')
            return row_count
    
    return process_azf_data(response_azf_download)

#   _______ .__   __.  _______          ___      ________   _______
# |   ____||  \ |  | |       \        /   \    |       /  |   ____|
# |  |__   |   \|  | |  .--.  |      /  ^  \   `---/  /   |  |__
# |   __|  |  . `  | |  |  |  |     /  /_\  \     /  /    |   __|
# |  |____ |  |\   | |  '--'  |    /  _____  \   /  /----.|  |
# |_______||__| \__| |_______/    /__/     \__\ /________||__|
    
# ---------------------------------------------------------------------------------


#  _______   ______   .______
# |   ____| /  __  \  |   _  \
# |  |__   |  |  |  | |  |_)  |
# |   __|  |  |  |  | |      /
# |  |     |  `--'  | |  |\  \----.
# |__|      \______/  | _| `._____|


def get_for_spreadsheet(access_token):
    
    for_search_param = urllib.parse.quote_plus("Forshaw Weekly Inventory Update")
    for_search_param2 = urllib.parse.quote_plus("wholesale")
    for_message_url = f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/messages/search?searchKey=subject:{for_search_param}::fileName:{for_search_param2}"

    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(for_message_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"FOR message request failed: {e}")
        return

    response_data = response.json()

    for_message_id = None
    for_email_received_date = None
    
    if 'data' in response_data:
        for_data = response_data['data']
        if isinstance(for_data, list) and len(for_data) > 0:
            first_element = for_data[0]
            if 'messageId' in first_element:
                for_message_id = first_element['messageId']
                append_log_messages(f'FOR Message ID: {for_message_id}')
                for_email_received_date_unix_epoch = first_element.get('receivedTime')
                if for_email_received_date_unix_epoch:
                    for_email_received_date = datetime.utcfromtimestamp(int(for_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
                    
    if not for_message_id:
        append_log_messages("No messages found for FOR.")
        return
    
    for_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{for_message_id}/attachmentinfo")
    
    try:
        response = requests.get(for_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"FOR attachment info request failed: {e}")
        return

    response_data = response.json()
    
    for_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        for_attachments = response_data['data']['attachments']
        for attachment in for_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                for_attachment_id = attachment['attachmentId']
                append_log_messages(f'FOR Attachment ID: {for_attachment_id}')
                break
    
    if not for_attachment_id:
        append_log_messages("No CSV attachment found for FOR.")
        return
    
    for_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{for_message_id}/attachments/{for_attachment_id}")
    
    try:
        response = requests.get(for_attachment_download_url, headers=headers)
        response.raise_for_status()
        response_text = response.text
    except RequestException as e:
        append_log_messages(f"FOR attachment download request failed: {e}")
        return
  
    if not response_text.endswith(","):
        response_text += ","

    rows = response_text.splitlines()

    def get_csv_fields(line):
        csv_reader = csv.reader([line])
        return next(csv_reader)
        
    csv_data = [get_csv_fields(row) for row in rows]

    csv_file_for = os.path.join(csv_folder_path, 'for.csv')

    with open(csv_file_for, 'w', newline='', encoding='utf-8') as fp:
        writer = csv.writer(fp)

        for_row_count = 0
        
        for fields in csv_data:
            if fields:
                writer.writerow(fields)
                for_row_count += 1
    
    append_log_messages(f'FOR Date Received: {for_email_received_date}')
    append_log_messages(f'FOR Rows: {for_row_count}')
        
        
        
        
# _______ .__   __.  _______      _______   ______   .______
# |   ____||  \ |  | |       \    |   ____| /  __  \  |   _  \
# |  |__   |   \|  | |  .--.  |   |  |__   |  |  |  | |  |_)  |
# |   __|  |  . `  | |  |  |  |   |   __|  |  |  |  | |      /
# |  |____ |  |\   | |  '--'  |   |  |     |  `--'  | |  |\  \----.
# |_______||__| \__| |_______/    |__|      \______/  | _| `._____|
                                                                     
       
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())