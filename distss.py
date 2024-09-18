import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QPushButton, QLabel, QVBoxLayout, QWidget, QCheckBox, QHBoxLayout
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QObject
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.alert import Alert
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
zoho_username_email = os.getenv("ZOHO_USERNAME_EMAIL")
zoho_password = os.getenv("ZOHO_PASSWORD")


# distributor logins
pcs_username = os.getenv("PCS_USERNAME")
pcs_password = os.getenv("PCS_PASSWORD")
pin_username = os.getenv("PIN_USERNAME")
pin_password = os.getenv("PIN_PASSWORD")


# other
log_messages = []
current_date = datetime.now().strftime("%Y%m%d")
current_date_w_dashes = datetime.now().strftime("%Y-%m-%d")
csv_folder_path = os.getenv("CSV_FOLDER_PATH")
download_dir = os.path.abspath(csv_folder_path)
pcs_file_name = download_dir + "\AvailQtyForSale_" + current_date + ".csv"
pin_file_name = download_dir + "\px_inventory_" + current_date_w_dashes + ".csv"
selenium_files_to_check_for = []


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
                                                                                              
                                                                                                           
#    __    __       ____      _____   _____            ____     _____     _____
#    \ \  / /      (    )    (_   _) (_   _)          (    )   (  __ \   (_   _)
#    () \/ ()      / /\ \      | |     | |            / /\ \    ) )_) )    | |
#    / _  _ \     ( (__) )     | |     | |           ( (__) )  (  ___/     | |
#   / / \/ \ \     )    (      | |     | |   __       )    (    ) )        | |
#  /_/      \_\   /  /\  \    _| |__ __| |___) )     /  /\  \  ( (        _| |__
# (/          \) /__(  )__\  /_____( \________/     /__(  )__\ /__\      /_____(
                                                                               
                                                                                                 
#--------------------------------------------------------------------------------------------------------


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
  

# ---------------------------------------------------------------------------------


# .______      .___  ___.  __
# |   _  \     |   \/   | |  |
# |  |_)  |    |  \  /  | |  |
# |      /     |  |\/|  | |  |
# |  |\  \----.|  |  |  | |  |
# | _| `._____||__|  |__| |__|
                                                                                              
  
def get_rmi_spreadsheet(access_token):
    rmi_search_param = urllib.parse.quote_plus("Daily inventory report from RMI attached")
    rmi_search_param2 = urllib.parse.quote_plus("INTRESRCE")
    rmi_message_url = f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/messages/search?searchKey=subject:{rmi_search_param}::fileName:{rmi_search_param2}"

    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(rmi_message_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"RMI message request failed: {e}")
        return

    response_data = response.json()

    rmi_message_id = None
    rmi_email_received_date = None
    
    if 'data' in response_data:
        rmi_data = response_data['data']
        if isinstance(rmi_data, list) and len(rmi_data) > 0:
            first_element = rmi_data[0]
            if 'messageId' in first_element:
                rmi_message_id = first_element['messageId']
                append_log_messages(f'RMI Message ID: {rmi_message_id}')
                rmi_email_received_date_unix_epoch = first_element.get('receivedTime')
                if rmi_email_received_date_unix_epoch:
                    rmi_email_received_date = datetime.utcfromtimestamp(int(rmi_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
                    
    if not rmi_message_id:
        append_log_messages("No messages found for RMI.")
        return
    
    rmi_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{rmi_message_id}/attachmentinfo")
    
    try:
        response = requests.get(rmi_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"RMI attachment info request failed: {e}")
        return

    response_data = response.json()
    
    rmi_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        rmi_attachments = response_data['data']['attachments']
        for attachment in rmi_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                rmi_attachment_id = attachment['attachmentId']
                append_log_messages(f'RMI Attachment ID: {rmi_attachment_id}')
                break
    
    if not rmi_attachment_id:
        append_log_messages("No CSV attachment found for RMI.")
        return
    
    rmi_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{rmi_message_id}/attachments/{rmi_attachment_id}")
    
    try:
        response = requests.get(rmi_attachment_download_url, headers=headers)
        response.raise_for_status()
        response_text = response.text
    except RequestException as e:
        append_log_messages(f"RMI attachment download request failed: {e}")
        return
  
    if not response_text.endswith(","):
        response_text += ","

    rows = response_text.splitlines()

    def get_csv_fields(line):
        csv_reader = csv.reader([line])
        return next(csv_reader)
        
    csv_data = [get_csv_fields(row) for row in rows]

    csv_file_rmi = os.path.join(csv_folder_path, 'rmi.csv')

    with open(csv_file_rmi, 'w', newline='', encoding='utf-8') as fp:
        writer = csv.writer(fp)

        rmi_row_count = 0
        
        for fields in csv_data:
            if fields:
                writer.writerow(fields)
                rmi_row_count += 1
    
    append_log_messages(f'RMI Date Received: {rmi_email_received_date}')
    append_log_messages(f'RMI Rows: {rmi_row_count}')
  
  
#  _______ .__   __.  _______     .______      .___  ___.  __
# |   ____||  \ |  | |       \    |   _  \     |   \/   | |  |
# |  |__   |   \|  | |  .--.  |   |  |_)  |    |  \  /  | |  |
# |   __|  |  . `  | |  |  |  |   |      /     |  |\/|  | |  |
# |  |____ |  |\   | |  '--'  |   |  |\  \----.|  |  |  | |  |
# |_______||__| \__| |_______/    | _| `._____||__|  |__| |__|
  
  
# ---------------------------------------------------------------------------------


# .______       __    __  .___________.
# |   _  \     |  |  |  | |           |
# |  |_)  |    |  |  |  | `---|  |----`
# |      /     |  |  |  |     |  |
# |  |\  \----.|  `--'  |     |  |
# | _| `._____| \______/      |__|


def get_rut_spreadsheet(access_token):
    rut_search_param = urllib.parse.quote_plus("Rutherford Equipment Inventory Feed")
    rut_message_url = f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/messages/search?searchKey=subject:{rut_search_param}"

    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(rut_message_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"RUT message request failed: {e}")
        return

    response_data = response.json()

    rut_message_id = None
    rut_email_received_date = None
    
    if 'data' in response_data:
        rut_data = response_data['data']
        if isinstance(rut_data, list) and len(rut_data) > 0:
            first_element = rut_data[0]
            if 'messageId' in first_element:
                rut_message_id = first_element['messageId']
                append_log_messages(f'RUT Message ID: {rut_message_id}')
                rut_email_received_date_unix_epoch = first_element.get('receivedTime')
                if rut_email_received_date_unix_epoch:
                    rut_email_received_date = datetime.utcfromtimestamp(int(rut_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
                    
    if not rut_message_id:
        append_log_messages("No messages found for RUT.")
        return
    
    rut_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{rut_message_id}/attachmentinfo")
    
    try:
        response = requests.get(rut_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"RUT attachment info request failed: {e}")
        return

    response_data = response.json()
    
    rut_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        rut_attachments = response_data['data']['attachments']
        for attachment in rut_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                rut_attachment_id = attachment['attachmentId']
                append_log_messages(f'RUT Attachment ID: {rut_attachment_id}')
                break
    
    if not rut_attachment_id:
        append_log_messages("No CSV attachment found for RUT.")
        return
    
    rut_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{rut_message_id}/attachments/{rut_attachment_id}")
    
    try:
        response = requests.get(rut_attachment_download_url, headers=headers)
        response.raise_for_status()
        response_text = response.text
    except RequestException as e:
        append_log_messages(f"RUT attachment download request failed: {e}")
        return
  
    if not response_text.endswith(","):
        response_text += ","

    rows = response_text.splitlines()

    def get_csv_fields(line):
        csv_reader = csv.reader([line])
        return next(csv_reader)
        
    csv_data = [get_csv_fields(row) for row in rows]

    csv_file_rut = os.path.join(csv_folder_path, 'rut.csv')

    with open(csv_file_rut, 'w', newline='', encoding='utf-8') as fp:
        writer = csv.writer(fp)

        rut_row_count = 0
        
        for fields in csv_data:
            if fields:
                writer.writerow(fields)
                rut_row_count += 1
    
    append_log_messages(f'RUT Date Received: {rut_email_received_date}')
    append_log_messages(f'RUT Rows: {rut_row_count}')


#  _______ .__   __.  _______     .______       __    __  .___________.
# |   ____||  \ |  | |       \    |   _  \     |  |  |  | |           |
# |  |__   |   \|  | |  .--.  |   |  |_)  |    |  |  |  | `---|  |----`
# |   __|  |  . `  | |  |  |  |   |      /     |  |  |  |     |  |
# |  |____ |  |\   | |  '--'  |   |  |\  \----.|  `--'  |     |  |
# |_______||__| \__| |_______/    | _| `._____| \______/      |__|
                                                                     

# ---------------------------------------------------------------------------------


# .___________.    _______. _______
# |           |   /       ||       \
# `---|  |----`  |   (----`|  .--.  |
#     |  |        \   \    |  |  |  |
#     |  |    .----)   |   |  '--'  |
#     |__|    |_______/    |_______/


def get_tsd_spreadsheet(access_token):
    tsd_search_param = urllib.parse.quote_plus("-")
    tsd_search_param2 = urllib.parse.quote_plus("IRG")
    tsd_message_url = f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/messages/search?searchKey=fileContent:{tsd_search_param}::fileName:{tsd_search_param2}"

    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(tsd_message_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"TSD message request failed: {e}")
        return

    response_data = response.json()

    tsd_message_id = None
    tsd_email_received_date = None
    
    if 'data' in response_data:
        tsd_data = response_data['data']
        if isinstance(tsd_data, list) and len(tsd_data) > 0:
            first_element = tsd_data[0]
            if 'messageId' in first_element:
                tsd_message_id = first_element['messageId']
                append_log_messages(f'TSD Message ID: {tsd_message_id}')
                tsd_email_received_date_unix_epoch = first_element.get('receivedTime')
                if tsd_email_received_date_unix_epoch:
                    tsd_email_received_date = datetime.utcfromtimestamp(int(tsd_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
                    
    if not tsd_message_id:
        append_log_messages("No messages found for TSD.")
        return
    
    tsd_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{tsd_message_id}/attachmentinfo")
    
    try:
        response = requests.get(tsd_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"TSD attachment info request failed: {e}")
        return

    response_data = response.json()
    
    tsd_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        tsd_attachments = response_data['data']['attachments']
        for attachment in tsd_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                tsd_attachment_id = attachment['attachmentId']
                append_log_messages(f'TSD Attachment ID: {tsd_attachment_id}')
                break
    
    if not tsd_attachment_id:
        append_log_messages("No CSV attachment found for TSD.")
        return
    
    tsd_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{tsd_message_id}/attachments/{tsd_attachment_id}")
    
    try:
        response = requests.get(tsd_attachment_download_url, headers=headers)
        response.raise_for_status()
        response_text = response.text
    except RequestException as e:
        append_log_messages(f"TSD attachment download request failed: {e}")
        return
  
    if not response_text.endswith(","):
        response_text += ","

    rows = response_text.splitlines()

    def get_csv_fields(line):
        csv_reader = csv.reader([line])
        return next(csv_reader)
        
    csv_data = [get_csv_fields(row) for row in rows]

    csv_file_tsd = os.path.join(csv_folder_path, 'tsd.csv')

    with open(csv_file_tsd, 'w', newline='', encoding='utf-8') as fp:
        writer = csv.writer(fp)

        tsd_row_count = 0
        
        for fields in csv_data:
            if fields:
                writer.writerow(fields)
                tsd_row_count += 1
    
    append_log_messages(f'TSD Date Received: {tsd_email_received_date}')
    append_log_messages(f'TSD Rows: {tsd_row_count}')
    


#  _______ .__   __.  _______     .___________.    _______. _______
# |   ____||  \ |  | |       \    |           |   /       ||       \
# |  |__   |   \|  | |  .--.  |   `---|  |----`  |   (----`|  .--.  |
# |   __|  |  . `  | |  |  |  |       |  |        \   \    |  |  |  |
# |  |____ |  |\   | |  '--'  |       |  |    .----)   |   |  '--'  |
# |_______||__| \__| |_______/        |__|    |_______/    |_______/
                                   

#--------------------------------------------------------------------------------------------------------


#   _____      __      _   ______          __    __       ____      _____   _____            ____     _____     _____
#  / ___/     /  \    / ) (_  __ \         \ \  / /      (    )    (_   _) (_   _)          (    )   (  __ \   (_   _)
# ( (__      / /\ \  / /    ) ) \ \        () \/ ()      / /\ \      | |     | |            / /\ \    ) )_) )    | |
#  ) __)     ) ) ) ) ) )   ( (   ) )       / _  _ \     ( (__) )     | |     | |           ( (__) )  (  ___/     | |
# ( (       ( ( ( ( ( (     ) )  ) )      / / \/ \ \     )    (      | |     | |   __       )    (    ) )        | |
#  \ \___   / /  \ \/ /    / /__/ /      /_/      \_\   /  /\  \    _| |__ __| |___) )     /  /\  \  ( (        _| |__
#   \____\ (_/    \__/    (______/      (/          \) /__(  )__\  /_____( \________/     /__(  )__\ /__\      /_____(
                                                                                                                     

#--------------------------------------------------------------------------------------------------------


#   _____    _____   _____        _____      __      _    _____   __    __     __    __
#  / ____\  / ___/  (_   _)      / ___/     /  \    / )  (_   _)  ) )  ( (     \ \  / /
# ( (___   ( (__      | |       ( (__      / /\ \  / /     | |   ( (    ) )    () \/ ()
#  \___ \   ) __)     | |        ) __)     ) ) ) ) ) )     | |    ) )  ( (     / _  _ \
#      ) ) ( (        | |   __  ( (       ( ( ( ( ( (      | |   ( (    ) )   / / \/ \ \
#  ___/ /   \ \___  __| |___) )  \ \___   / /  \ \/ /     _| |__  ) \__/ (   /_/      \_\
# /____/     \____\ \________/    \____\ (_/    \__/     /_____(  \______/  (/          \)
                                      

#--------------------------------------------------------------------------------------------------------


# .______     ______     _______.
# |   _  \   /      |   /       |
# |  |_)  | |  ,----'  |   (----`
# |   ___/  |  |        \   \
# |  |      |  `----.----)   |
# | _|       \______|_______/


#--------------------------------------------------------------------------------------------------------


def get_pcs_spreadsheet(i):

    options = Options()
    options.add_argument('--enable-logging')
    options.add_argument('--v=1')

    prefs = {
        'download.default_directory': download_dir,
        'download.prompt_for_download': False,
        'directory_upgrade': True,
        'safebrowsing.enabled': True
    }

    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)
    driver.get("https://palmcoastorders.lp4fb.com/index.php?route=account/login")
    wait.until(EC.presence_of_element_located((By.ID, "username"))).send_keys(pcs_username)
    wait.until(EC.presence_of_element_located((By.ID, "password"))).send_keys(pcs_password)
    wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div/div[1]/div/div/div[2]/form/div[3]/button"))).click()
    time.sleep(1.25)
        
    # downloading the spreadsheet in a new tab
    driver.execute_script(
        '''window.open("https://palmcoastorders.lp4fb.com/index.php?route=report/report/csv/avail-for-sale&lg%5B%5D=1","_blank");'''
    )
    

    timeout = 30
    elapsed = 0
    sleep_interval = 0.5

    while not os.path.exists(pcs_file_name) and elapsed < timeout:
        time.sleep(sleep_interval)
        elapsed += sleep_interval

    if os.path.exists(pcs_file_name):
        append_log_messages("PCS File Downloaded")
        # renaming file to standard format
        os.rename(pcs_file_name, download_dir + "\pcs.csv")
    else:
        append_log_messages(f"PCS File not found after waiting {timeout} seconds.")
    
    driver.quit()


#  _______ .__   __.  _______     .______     ______     _______.
# |   ____||  \ |  | |       \    |   _  \   /      |   /       |
# |  |__   |   \|  | |  .--.  |   |  |_)  | |  ,----'  |   (----`
# |   __|  |  . `  | |  |  |  |   |   ___/  |  |        \   \
# |  |____ |  |\   | |  '--'  |   |  |      |  `----.----)   |
# |_______||__| \__| |_______/    | _|       \______|_______/


#--------------------------------------------------------------------------------------------------------


# .______    __  .__   __.
# |   _  \  |  | |  \ |  |
# |  |_)  | |  | |   \|  |
# |   ___/  |  | |  . `  |
# |  |      |  | |  |\   |
# | _|      |__| |__| \__|


def get_pin_spreadsheet(i):

    options = Options()
    options.add_argument('--enable-logging')
    options.add_argument('--v=1')

    prefs = {
        'download.default_directory': download_dir,
        'download.prompt_for_download': False,
        'directory_upgrade': True,
        'safebrowsing.enabled': True
    }

    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)
    
    driver.get("https://pinnaclesalesgroup.com/membership-login/")
    wait.until(EC.presence_of_element_located((By.ID, "swpm_user_name"))).send_keys(pin_username)
    wait.until(EC.presence_of_element_located((By.ID, "swpm_password"))).send_keys(pin_password)

    wait.until(EC.element_to_be_clickable((By.NAME, "swpm-login"))).click()
    wait.until(EC.presence_of_element_located((By.NAME, "searchID"))).click()


    wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[3]/div/div/div/div[1]/div/div/article/div/div/div/div/div[1]/div/div[2]/div/article/div[2]/div/div/article/form/div/nav[1]/ul/li/a[2]")))
    wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[3]/div/div/div/div[1]/div/div/article/div/div/div/div/div[1]/div/div[2]/div/article/div[2]/div/div/article/form/div/nav[1]/ul/li/a[2]"))).click()
    wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[3]/div/div[2]")))
    wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[3]/div/div[2]"))).click()

    timeout = 30
    elapsed = 0
    sleep_interval = 0.5

    while not os.path.exists(pin_file_name) and elapsed < timeout:
        time.sleep(sleep_interval)
        elapsed += sleep_interval

    if os.path.exists(pin_file_name):
        append_log_messages("PIN File Downloaded")
        # renaming file to standard format
        os.rename(pin_file_name, download_dir + "\pin.csv")
    else:
        append_log_messages(f"PIN File not found after waiting {timeout} seconds.")
    
    driver.quit()


#  _______ .__   __.  _______     .______    __  .__   __.
# |   ____||  \ |  | |       \    |   _  \  |  | |  \ |  |
# |  |__   |   \|  | |  .--.  |   |  |_)  | |  | |   \|  |
# |   __|  |  . `  | |  |  |  |   |   ___/  |  | |  . `  |
# |  |____ |  |\   | |  '--'  |   |  |      |  | |  |\   |
# |_______||__| \__| |_______/    | _|      |__| |__| \__|
                                                         

#--------------------------------------------------------------------------------------------------------


#  __    __  .______    __        ______        ___       _______   __  .__   __.   _______
# |  |  |  | |   _  \  |  |      /  __  \      /   \     |       \ |  | |  \ |  |  /  _____|
# |  |  |  | |  |_)  | |  |     |  |  |  |    /  ^  \    |  .--.  ||  | |   \|  | |  |  __
# |  |  |  | |   ___/  |  |     |  |  |  |   /  /_\  \   |  |  |  ||  | |  . `  | |  | |_ |
# |  `--'  | |  |      |  `----.|  `--'  |  /  _____  \  |  '--'  ||  | |  |\   | |  |__| |
#  \______/  | _|      |_______| \______/  /__/     \__\ |_______/ |__| |__| \__|  \______|
                                                                                                         
# .___________.  ______        ______ .______       _______     ___   .___________.  ______   .______
# |           | /  __  \      /      ||   _  \     |   ____|   /   \  |           | /  __  \  |   _  \
# `---|  |----`|  |  |  |    |  ,----'|  |_)  |    |  |__     /  ^  \ `---|  |----`|  |  |  | |  |_)  |
#     |  |     |  |  |  |    |  |     |      /     |   __|   /  /_\  \    |  |     |  |  |  | |      /
#     |  |     |  `--'  |    |  `----.|  |\  \----.|  |____ /  _____  \   |  |     |  `--'  | |  |\  \----.
#     |__|      \______/      \______|| _| `._____||_______/__/     \__\  |__|      \______/  | _| `._____|


def upload_to_creator():
    print('uploading...')
                                                                                                                                                                                                                                                                                


#   _____      __      _   ______         _____    _____   _____        _____      __      _    _____   __    __     __    __
#  / ___/     /  \    / ) (_  __ \       / ____\  / ___/  (_   _)      / ___/     /  \    / )  (_   _)  ) )  ( (     \ \  / /
# ( (__      / /\ \  / /    ) ) \ \     ( (___   ( (__      | |       ( (__      / /\ \  / /     | |   ( (    ) )    () \/ ()
#  ) __)     ) ) ) ) ) )   ( (   ) )     \___ \   ) __)     | |        ) __)     ) ) ) ) ) )     | |    ) )  ( (     / _  _ \
# ( (       ( ( ( ( ( (     ) )  ) )         ) ) ( (        | |   __  ( (       ( ( ( ( ( (      | |   ( (    ) )   / / \/ \ \
#  \ \___   / /  \ \/ /    / /__/ /      ___/ /   \ \___  __| |___) )  \ \___   / /  \ \/ /     _| |__  ) \__/ (   /_/      \_\
#   \____\ (_/    \__/    (______/      /____/     \____\ \________/    \____\ (_/    \__/     /_____(  \______/  (/          \)
                                                                                                                               





#                                    ___
#                                   (   )
#    .-..     .---.   ___ .-.     .-.| |    .---.      .--.
#   /    \   / .-, \ (   )   \   /   \ |   / .-, \   /  _  \
#  ' .-,  ; (__) ; |  |  .-. .  |  .-. |  (__) ; |  . .' `. ;
#  | |  . |   .'`  |  | |  | |  | |  | |    .'`  |  | '   | |
#  | |  | |  / .'| |  | |  | |  | |  | |   / .'| |  _\_`.(___)
#  | |  | | | /  | |  | |  | |  | |  | |  | /  | | (   ). '.
#  | |  ' | ; |  ; |  | |  | |  | '  | |  ; |  ; |  | |  `\ |
#  | `-'  ' ' `-'  |  | |  | |  ' `-'  /  ' `-'  |  ; '._,' '
#  | \__.'  `.__.'_. (___)(___)  `.__,'   `.__.'_.   '.___.'
#  | |
# (___)
                                                                              
                                                                                               
                                                                        
def pandas():
    print('pandas')
  
                                                                                          
#                          ___                                       ___
#                         (   )                                     (   )
#   .--.    ___ .-.     .-.| |       .-..     .---.   ___ .-.     .-.| |    .---.      .--.
#  /    \  (   )   \   /   \ |      /    \   / .-, \ (   )   \   /   \ |   / .-, \   /  _  \
# |  .-. ;  |  .-. .  |  .-. |     ' .-,  ; (__) ; |  |  .-. .  |  .-. |  (__) ; |  . .' `. ;
# |  | | |  | |  | |  | |  | |     | |  . |   .'`  |  | |  | |  | |  | |    .'`  |  | '   | |
# |  |/  |  | |  | |  | |  | |     | |  | |  / .'| |  | |  | |  | |  | |   / .'| |  _\_`.(___)
# |  ' _.'  | |  | |  | |  | |     | |  | | | /  | |  | |  | |  | |  | |  | /  | | (   ). '.
# |  .'.-.  | |  | |  | '  | |     | |  ' | ; |  ; |  | |  | |  | '  | |  ; |  ; |  | |  `\ |
# '  `-' /  | |  | |  ' `-'  /     | `-'  ' ' `-'  |  | |  | |  ' `-'  /  ' `-'  |  ; '._,' '
#  `.__.'  (___)(___)  `.__,'      | \__.'  `.__.'_. (___)(___)  `.__,'   `.__.'_.   '.___.'
#                                  | |
#                                 (___)
                                                                                          
                                                                                          



if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())