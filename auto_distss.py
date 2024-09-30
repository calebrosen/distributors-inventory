import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QPushButton, QLabel, QVBoxLayout, QWidget, QCheckBox, QHBoxLayout, QScrollArea
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
import os
import io
import time
import threading
import smtplib
import openpyxl
import mysql.connector
from mysql.connector import Error
from email.mime.text import MIMEText
import re
from email.mime.multipart import MIMEMultipart
import requests
import csv
import json
from urllib.parse import quote, urlencode
import requests
import pandas as pd
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
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
it_email = os.getenv("IT_EMAIL")


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
current_date_mm_dd_yyyy =datetime.now().strftime('%m/%d/%Y')
current_date = datetime.now().strftime("%Y%m%d")
current_date_w_dashes = datetime.now().strftime("%Y-%m-%d")
csv_folder_path = os.getenv("CSV_FOLDER_PATH")
download_dir = os.path.abspath(csv_folder_path)
master_file_path = os.path.join(download_dir, "master_file.csv")
formatted_dfs_queue = Queue()
master_row_count = 0
print_lock = threading.Lock()
pcs_file_name = download_dir + "\AvailQtyForSale_" + current_date + ".csv"
pin_file_name = download_dir + "\px_inventory_" + current_date_w_dashes + ".csv"
mysql_host = os.getenv("MYSQL_HOST")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_db = os.getenv("MYSQL_DB")

# ███████╗███╗   ██╗██████╗     ██╗   ██╗ █████╗ ██████╗ ██╗ █████╗ ██████╗ ██╗     ███████╗███████╗
# ██╔════╝████╗  ██║██╔══██╗    ██║   ██║██╔══██╗██╔══██╗██║██╔══██╗██╔══██╗██║     ██╔════╝██╔════╝
# █████╗  ██╔██╗ ██║██║  ██║    ██║   ██║███████║██████╔╝██║███████║██████╔╝██║     █████╗  ███████╗
# ██╔══╝  ██║╚██╗██║██║  ██║    ╚██╗ ██╔╝██╔══██║██╔══██╗██║██╔══██║██╔══██╗██║     ██╔══╝  ╚════██║
# ███████╗██║ ╚████║██████╔╝     ╚████╔╝ ██║  ██║██║  ██║██║██║  ██║██████╔╝███████╗███████╗███████║
# ╚══════╝╚═╝  ╚═══╝╚═════╝       ╚═══╝  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚══════╝╚══════╝
                                               
def send_success_email():
    sender_email = gmail_user
    receiver_email = it_email
    subject = "Automatic Inventory Spreadsheets Finished"
    body = f"Inventory spreadsheets finished:\n\nTotal Rows Imported: {master_row_count}"

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
        print("Success email sent successfully!")
    except Exception as e:
        print("Error sending success email:", e)
    finally:
        server.quit()
                              
                                                                                                                                                                                               
def send_error_email(error_message):
    sender_email = gmail_user
    receiver_email = it_email
    subject = "Automatic Inventory Spreadsheet Python Error"
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
        print("Error email sent successfully!")
    except Exception as e:
        print("Error sending error email:", e)
    finally:
        server.quit()
    
    
def append_log_messages(log_message, goodOrBad):
    global log_messages
    log_messages.append(log_message)
    
    # passing 1 to this functions means something bad happened,
    # so I am sending an email to IT inbox with it
    if (goodOrBad == 1):
        send_error_email(log_message)
    
    print(f"Log appended: {log_message}")


class WorkerSignals(QObject):
    update_log_display = pyqtSignal()
   
    
class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        
    
        # this is all main UI layout, should not have to be touched even with future implementations
        
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


        scroll_area = QScrollArea(self)
        scroll_area.setWidgetResizable(True)

        self.log_container = QWidget()
        scroll_layout = QVBoxLayout(self.log_container)

        
        self.label = QLabel("", self)
        self.label.setStyleSheet(
            "background-color: black; color: white; font-family: 'Courier'; font-size: 14px;")
        self.label.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.label.setWordWrap(True)

        scroll_layout.addWidget(self.label)

        scroll_area.setWidget(self.log_container)

        main_layout.addWidget(scroll_area)

        button_layout = QHBoxLayout()
        self.downloadSelectedButton = QPushButton("Download Selected", self)
        self.downloadSelectedAndUploadToCreatorButton = QPushButton(
            "Downloaded Selected and Upload to Creator", self)
        button_layout.addWidget(self.downloadSelectedButton)
        button_layout.addWidget(self.downloadSelectedAndUploadToCreatorButton)
        main_layout.addLayout(button_layout)

        checkbox_layout = QHBoxLayout()
        self.checkboxes = []
        checkbox_labels = ["AES", "AZF", "FOR", "IRG", "RMI", "RUT", "TSD", "PIN", "PCS"]
        for label in checkbox_labels:
            checkbox = QCheckBox(label, self)
            checkbox.setChecked(True)
            checkbox.stateChanged.connect(self.update_selected_distributors)
            checkbox_layout.addWidget(checkbox)
            self.checkboxes.append(checkbox)

        main_layout.addLayout(checkbox_layout)

        self.downloadSelectedButton.clicked.connect(self.downloadSelected)
        self.downloadSelectedAndUploadToCreatorButton.clicked.connect(
            self.downloadSelectedAndUploadToCreator)

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
        
                
        # automatically running function on program opening
        self.downloadSelectedAndUploadToCreatorButton.click()
        
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
        append_log_messages(f"Selected distributors: {selected_distributors}", 0)
 
    def downloadSelectedAndUploadToCreator(self):
        # getting access token
        self.getAccessTokenFromRefreshToken()

        def update_log_display_thread():
            self.signals.update_log_display.emit()

        def process_distributor(dist):
            
            # this processes each distributor
            func_name = f"get_{dist.lower()}_spreadsheet"
            func = globals().get(func_name)
            if callable(func):
                func(self.access_token)
            else:
                print(f"Function {func_name} not found")

            update_thread = threading.Thread(target=update_log_display_thread)
            update_thread.start()

        threads = []
        for dist in selected_distributors:
            # creating new thread for each dist
            thread = threading.Thread(target=process_distributor, args=(dist,))
            threads.append(thread)
            thread.start()

        # ensuring all threads finish before proceeding
        for thread in threads:
            thread.join()


        # after everything is done
        get_csv_files(selected_distributors)
        
        if (master_row_count > 0):
            upload_to_creator()
        import_csv_to_mysql("distributors_availability", master_file_path)
        import_csv_to_mysql("irg_warehouse", f"{download_dir}/irg_formatted.csv")





    def downloadSelected(self):
        # getting access token
        self.getAccessTokenFromRefreshToken()

        def update_log_display_thread():
            self.signals.update_log_display.emit()

        def process_distributor(dist):
            
            # this processes each distributor
            func_name = f"get_{dist.lower()}_spreadsheet"
            func = globals().get(func_name)
            if callable(func):
                func(self.access_token)
            else:
                print(f"Function {func_name} not found")

            update_thread = threading.Thread(target=update_log_display_thread)
            update_thread.start()

        threads = []
        for dist in selected_distributors:
            # creating new thread for each dist
            thread = threading.Thread(target=process_distributor, args=(dist,))
            threads.append(thread)
            thread.start()

        # ensuring all threads finish before proceeding
        for thread in threads:
            thread.join()


        # after everything is done
        get_csv_files(selected_distributors)
        import_csv_to_mysql("distributors_availability", master_file_path)
        import_csv_to_mysql("irg_warehouse", f"{download_dir}/irg_formatted.csv")



    def getAccessTokenFromRefreshToken(self):
        try:
            append_log_messages("Retrieving token...", 0)
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
                append_log_messages(f"Access token: {access_token}", 0)
                
        except Exception as e:
            print("Error getting access token:", e)
            append_log_messages(f"Error getting access token: {e}", 1)
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
        append_log_messages(f"AES Message request failed: {e}", 1)
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
                append_log_messages(f'AES Message ID: {aes_message_id}', 0)
                aes_email_received_date_unix_epoch = first_element.get('receivedTime')
                if aes_email_received_date_unix_epoch:
                    aes_email_received_date = datetime.utcfromtimestamp(int(aes_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
    if not aes_message_id:
        append_log_messages("No AES messages found.", 1)
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
                append_log_messages(f'AES Attachment ID: {aes_attachment_id}', 0)
                break
    
    if not aes_attachment_id:
        append_log_messages("No AES CSV attachment found.", 1)
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
        append_log_messages(f"AES Date Received: {aes_email_received_date}", 0)
        append_log_messages(f"AES Rows: {row_count}", 0)
    else:
        append_log_messages(f"No data was written to {csv_file_aes}", 1)


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
        append_log_messages(f"AZF Message request failed: {e}", 1)
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
                append_log_messages(f'AZF Message ID: {azf_message_id}', 0)
                azf_email_received_date_unix_epoch = first_element.get('receivedTime')
                if azf_email_received_date_unix_epoch:
                    azf_email_received_date = datetime.utcfromtimestamp(int(azf_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
    if not azf_message_id:
        append_log_messages("No AZF message found.", 1)
        return
    
    azf_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{azf_message_id}/attachmentinfo")
    
    try:
        response = requests.get(azf_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"AZF Attachment Info failed: {e}", 1)
        return

    response_data = response.json()
    
    azf_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        azf_attachments = response_data['data']['attachments']
        for attachment in azf_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                azf_attachment_id = attachment['attachmentId']
                append_log_messages(f'AZF Attachment ID: {azf_attachment_id}', 0)
                break
    
    if not azf_attachment_id:
        append_log_messages("No AZF CSV attachment found.", 1)
        return
    
    azf_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{azf_message_id}/attachments/{azf_attachment_id}")
    
    try:
        response = requests.get(azf_attachment_download_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"AZF attachment download failed: {e}", 1)
        return

    response_azf_download = response.content.decode('utf-16')

    csv_file_azf = os.path.join(csv_folder_path, 'azf.csv')

    def get_csv_fields_azf(line):
        return list(csv.reader(io.StringIO(line), delimiter='\t' if '\t' in line else ','))[0]

    def process_azf_data(response_azf_download):
        azf_rows = [line for line in response_azf_download.splitlines() if line.strip()]

        if len(azf_rows) > 1:
            headers_azf = get_csv_fields_azf(azf_rows.pop(0))

            bad_data_values = {'NULL', 'NaN', 'N/A', '', ' '}

            csv_data_azf = [get_csv_fields_azf(row) for row in azf_rows]


            processed_csv_data_azf = [
                row for row in csv_data_azf
                if not all(re.fullmatch(r'-+', field.strip()) for field in row)  # removing rows where all fields are only dashes
            ]

            # cleaning bad data
            cleaned_csv_data_azf = [
                [field if field.strip() not in bad_data_values else '0' for field in row] for row in processed_csv_data_azf
            ]

            # writing cleaned data
            with open(csv_file_azf, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                row_count = 0

                if headers_azf:
                    writer.writerow(headers_azf)
                    row_count += 1

                for fields_azf in cleaned_csv_data_azf:
                    writer.writerow(fields_azf)
                    row_count += 1

            append_log_messages(f'AZF Date Received: {azf_email_received_date}', 0)
            append_log_messages(f'AZF Rows: {row_count}', 0)
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
        append_log_messages(f"FOR message request failed: {e}", 1)
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
                append_log_messages(f'FOR Message ID: {for_message_id}', 0)
                for_email_received_date_unix_epoch = first_element.get('receivedTime')
                if for_email_received_date_unix_epoch:
                    for_email_received_date = datetime.utcfromtimestamp(int(for_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
                    
    if not for_message_id:
        append_log_messages("No messages found for FOR.", 1)
        return
    
    for_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{for_message_id}/attachmentinfo")
    
    try:
        response = requests.get(for_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"FOR attachment info request failed: {e}", 1)
        return

    response_data = response.json()
    
    for_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        for_attachments = response_data['data']['attachments']
        for attachment in for_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                for_attachment_id = attachment['attachmentId']
                append_log_messages(f'FOR Attachment ID: {for_attachment_id}', 0)
                break
    
    if not for_attachment_id:
        append_log_messages("No CSV attachment found for FOR.", 1)
        return
    
    for_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{for_message_id}/attachments/{for_attachment_id}")
    
    try:
        response = requests.get(for_attachment_download_url, headers=headers)
        response.raise_for_status()
        response_text = response.text
    except RequestException as e:
        append_log_messages(f"FOR attachment download request failed: {e}", 1)
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
    
    append_log_messages(f'FOR Date Received: {for_email_received_date}', 0)
    append_log_messages(f'FOR Rows: {for_row_count}', 0)
        

# _______ .__   __.  _______      _______   ______   .______
# |   ____||  \ |  | |       \    |   ____| /  __  \  |   _  \
# |  |__   |   \|  | |  .--.  |   |  |__   |  |  |  | |  |_)  |
# |   __|  |  . `  | |  |  |  |   |   __|  |  |  |  | |      /
# |  |____ |  |\   | |  '--'  |   |  |     |  `--'  | |  |\  \----.
# |_______||__| \__| |_______/    |__|      \______/  | _| `._____|
  

# ---------------------------------------------------------------------------------


#  __  .______        _______
# |  | |   _  \      /  _____|
# |  | |  |_)  |    |  |  __
# |  | |      /     |  | |_ |
# |  | |  |\  \----.|  |__| |
# |__| | _| `._____| \______|


def get_irg_spreadsheet(access_token):
    irg_search_param = urllib.parse.quote_plus('Check out the "Warehouse Available Items" report')
    irg_search_param2 = urllib.parse.quote_plus("Warehouse_Available_Items.csv")
    irg_message_url = f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/messages/search?searchKey=subject:{irg_search_param}::fileName:{irg_search_param2}"

    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(irg_message_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"IRG message request failed: {e}", 1)
        return

    response_data = response.json()

    irg_message_id = None
    irg_email_received_date = None
    
    if 'data' in response_data:
        irg_data = response_data['data']
        if isinstance(irg_data, list) and len(irg_data) > 0:
            first_element = irg_data[0]
            if 'messageId' in first_element:
                irg_message_id = first_element['messageId']
                append_log_messages(f'IRG Message ID: {irg_message_id}', 0)
                irg_email_received_date_unix_epoch = first_element.get('receivedTime')
                if irg_email_received_date_unix_epoch:
                    irg_email_received_date = datetime.utcfromtimestamp(int(irg_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
                    
    if not irg_message_id:
        append_log_messages("No messages found for IRG.", 1)
        return
    
    irg_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{irg_message_id}/attachmentinfo")
    
    try:
        response = requests.get(irg_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"FOR attachment info request failed: {e}", 1)
        return

    response_data = response.json()
    
    irg_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        irg_attachments = response_data['data']['attachments']
        for attachment in irg_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                irg_attachment_id = attachment['attachmentId']
                append_log_messages(f'IRG Attachment ID: {irg_attachment_id}', 0)
                break
    
    if not irg_attachment_id:
        append_log_messages("No CSV attachment found for IRG.", 1)
        return
    
    irg_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{irg_message_id}/attachments/{irg_attachment_id}")
    
    try:
        response = requests.get(irg_attachment_download_url, headers=headers)
        response.raise_for_status()
        response_text = response.text
    except RequestException as e:
        append_log_messages(f"IRG attachment download request failed: {e}", 1)
        return
  
    if not response_text.endswith(","):
        response_text += ","

    rows = response_text.splitlines()

    def get_csv_fields(line):
        csv_reader = csv.reader([line])
        return next(csv_reader)
        
    csv_data = [get_csv_fields(row) for row in rows]

    csv_file_irg = os.path.join(csv_folder_path, 'irg.csv')

    with open(csv_file_irg, 'w', newline='', encoding='utf-8') as fp:
        writer = csv.writer(fp)

        irg_row_count = 0
        
        for fields in csv_data:
            if fields:
                writer.writerow(fields)
                irg_row_count += 1
    
    append_log_messages(f'IRG Date Received: {irg_email_received_date}', 0)
    append_log_messages(f'IRG Rows: {irg_row_count}', 0)


#  _______ .__   __.  _______      __  .______        _______
# |   ____||  \ |  | |       \    |  | |   _  \      /  _____|
# |  |__   |   \|  | |  .--.  |   |  | |  |_)  |    |  |  __
# |   __|  |  . `  | |  |  |  |   |  | |      /     |  | |_ |
# |  |____ |  |\   | |  '--'  |   |  | |  |\  \----.|  |__| |
# |_______||__| \__| |_______/    |__| | _| `._____| \______|


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
        append_log_messages(f"RMI message request failed: {e}", 1)
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
                append_log_messages(f'RMI Message ID: {rmi_message_id}', 0)
                rmi_email_received_date_unix_epoch = first_element.get('receivedTime')
                if rmi_email_received_date_unix_epoch:
                    rmi_email_received_date = datetime.utcfromtimestamp(int(rmi_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
                    
    if not rmi_message_id:
        append_log_messages("No messages found for RMI.", 1)
        return
    
    rmi_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{rmi_message_id}/attachmentinfo")
    
    try:
        response = requests.get(rmi_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"RMI attachment info request failed: {e}", 1)
        return

    response_data = response.json()
    
    rmi_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        rmi_attachments = response_data['data']['attachments']
        for attachment in rmi_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                rmi_attachment_id = attachment['attachmentId']
                append_log_messages(f'RMI Attachment ID: {rmi_attachment_id}', 0)
                break
    
    if not rmi_attachment_id:
        append_log_messages("No CSV attachment found for RMI.", 1)
        return
    
    rmi_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{rmi_message_id}/attachments/{rmi_attachment_id}")
    
    try:
        response = requests.get(rmi_attachment_download_url, headers=headers)
        response.raise_for_status()
        response_text = response.text
    except RequestException as e:
        append_log_messages(f"RMI attachment download request failed: {e}", 1)
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
    
    append_log_messages(f'RMI Date Received: {rmi_email_received_date}', 0)
    append_log_messages(f'RMI Rows: {rmi_row_count}', 0)
  
  
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
        append_log_messages(f"RUT message request failed: {e}", 1)
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
                append_log_messages(f'RUT Message ID: {rut_message_id}', 0)
                rut_email_received_date_unix_epoch = first_element.get('receivedTime')
                if rut_email_received_date_unix_epoch:
                    rut_email_received_date = datetime.utcfromtimestamp(int(rut_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
                    
    if not rut_message_id:
        append_log_messages("No messages found for RUT.", 1)
        return
    
    rut_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{rut_message_id}/attachmentinfo")
    
    try:
        response = requests.get(rut_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"RUT attachment info request failed: {e}", 1)
        return

    response_data = response.json()
    
    rut_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        rut_attachments = response_data['data']['attachments']
        for attachment in rut_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                rut_attachment_id = attachment['attachmentId']
                append_log_messages(f'RUT Attachment ID: {rut_attachment_id}', 0)
                break
    
    if not rut_attachment_id:
        append_log_messages("No CSV attachment found for RUT.", 1)
        return
    
    rut_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{rut_message_id}/attachments/{rut_attachment_id}")
    
    try:
        response = requests.get(rut_attachment_download_url, headers=headers)
        response.raise_for_status()
        response_text = response.text
    except RequestException as e:
        append_log_messages(f"RUT attachment download request failed: {e}", 1)
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
    
    append_log_messages(f'RUT Date Received: {rut_email_received_date}', 0)
    append_log_messages(f'RUT Rows: {rut_row_count}', 0)


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
        append_log_messages(f"TSD message request failed: {e}", 1)
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
                append_log_messages(f'TSD Message ID: {tsd_message_id}', 0)
                tsd_email_received_date_unix_epoch = first_element.get('receivedTime')
                if tsd_email_received_date_unix_epoch:
                    tsd_email_received_date = datetime.utcfromtimestamp(int(tsd_email_received_date_unix_epoch) / 1000).strftime('%m/%d/%Y')
                    
    if not tsd_message_id:
        append_log_messages("No messages found for TSD.", 1)
        return
    
    tsd_attachment_info_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                               f"messages/{tsd_message_id}/attachmentinfo")
    
    try:
        response = requests.get(tsd_attachment_info_url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        append_log_messages(f"TSD attachment info request failed: {e}", 1)
        return

    response_data = response.json()
    
    tsd_attachment_id = None
    
    if 'data' in response_data and 'attachments' in response_data['data']:
        tsd_attachments = response_data['data']['attachments']
        for attachment in tsd_attachments:
            if attachment['attachmentName'].endswith(".csv"):
                tsd_attachment_id = attachment['attachmentId']
                append_log_messages(f'TSD Attachment ID: {tsd_attachment_id}', 0)
                break
    
    if not tsd_attachment_id:
        append_log_messages("No CSV attachment found for TSD.", 1)
        return
    
    tsd_attachment_download_url = (f"https://mail.zoho.com/api/accounts/{zoho_mail_account_id}/folders/{zoho_mail_folder_id}/"
                                   f"messages/{tsd_message_id}/attachments/{tsd_attachment_id}")
    
    try:
        response = requests.get(tsd_attachment_download_url, headers=headers)
        response.raise_for_status()
        response_text = response.text
    except RequestException as e:
        append_log_messages(f"TSD attachment download request failed: {e}", 1)
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
    
    append_log_messages(f'TSD Date Received: {tsd_email_received_date}', 0)
    append_log_messages(f'TSD Rows: {tsd_row_count}', 0)
    


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

    # making sure file exists
    if os.path.exists(pcs_file_name):
        append_log_messages("PCS File Downloaded", 0)

        destination_file = os.path.join(download_dir, "pcs.csv")

        # deleting old file if its exists
        if os.path.exists(destination_file):
            os.remove(destination_file)
            append_log_messages("Old pcs.csv file deleted", 0)

        # renaming file
        os.rename(pcs_file_name, destination_file)
        append_log_messages("File renamed to pcs.csv", 0)

    else:
        # file wasn't found for some reason
        append_log_messages(f"PCS File not found after waiting {timeout} seconds.", 1)
    
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

    # making sure file exists
    if os.path.exists(pin_file_name):
        append_log_messages("PIN File Downloaded", 0)

        destination_file = os.path.join(download_dir, "pin.csv")

        # deleting old file if its exists
        if os.path.exists(destination_file):
            os.remove(destination_file)
            append_log_messages("Old pin.csv file deleted", 0)

        # renaming file
        os.rename(pin_file_name, destination_file)
        append_log_messages("File renamed to pin.csv", 0)

    else:
        # file wasn't found for some reason
        append_log_messages(f"PIN File not found after waiting {timeout} seconds.", 1)
    
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
    try:
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
        wait = WebDriverWait(driver, 30)
        driver.get("https://creatorapp.zoho.com/internetresourcegroup/backorder-application#Form:Delete_Distributors_Stock")

        # logging in
        zoho_user_name = wait.until(EC.element_to_be_clickable((By.ID, "login_id")))
        zoho_user_name.send_keys(zoho_username_email)
        driver.find_element(By.ID, "nextbtn").click()

        zoho_password_field = wait.until(EC.element_to_be_clickable((By.ID, "password")))
        zoho_password_field.send_keys(zoho_password)
        driver.find_element(By.ID, "nextbtn").click()

        # since the page is technically available but it's just a loading indicator, waiting a few seconds
        time.sleep(8)
        
        try:
            remind_me_later = driver.find_element(By.CLASS_NAME, "remind_me_later")
            if remind_me_later.is_displayed():
                remind_me_later.click()
        except NoSuchElementException:
            pass
        except TimeoutException:
            pass

        try:
            close_session_element = driver.find_element(By.ID, "close_session")
            if close_session_element.is_displayed():
                close_session_element.click()
        except NoSuchElementException:
            pass
        except TimeoutException:
            pass

        try:
            doitlater = driver.find_element(By.CLASS_NAME, "dolater")
            if doitlater.is_displayed():
                doitlater.click()
        except NoSuchElementException:
            pass
        except TimeoutException:
            pass

        # deleting all
        wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'label[elname="Everything_1"]'))).click()
        deleteButton = wait.until(EC.element_to_be_clickable((By.NAME, "Delete_field")))
        deleteButton.click()
        time.sleep(1.5)

        # waiting for delete to complete
        waitForNotDisabled = WebDriverWait(driver, 50)
        waitForNotDisabled.until(
            lambda d: d.execute_script(
                "return window.getComputedStyle(document.getElementById('preloader')).display"
            ) == "none",
            message="Waiting for deletion to complete"
        )

        
        wait.until(EC.element_to_be_clickable((By.ID, "tab_Distributors_Inventory"))).click()

        # removing any search filters if they're present
        try:
            searchFilterX = driver.find_element(By.ID, "cancelCriteria")
            if searchFilterX.is_displayed():
                searchFilterX.click()
        except NoSuchElementException:
            pass
        except TimeoutException:
            pass

        # clicking the import button
        importButton = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "zc-importdata-report")))
        importButton.click()

        # switching to iframe
        iFrame = wait.until(EC.presence_of_element_located((By.NAME, "sheet2AppIFRAME")))
        driver.switch_to.frame(iFrame)

        # uploading file
        fileUpload = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
        fileUpload.send_keys(master_file_path)

        # closing side panel
        wait.until(EC.element_to_be_clickable((By.ID, "SidePanelClose"))).click()

        # starting upload process
        wait.until(EC.element_to_be_clickable((By.ID, "s2a-create"))).click()

        time.sleep(2.5)
        
        try:
            import_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button.ZsAlertBtn.ZsAlertBtnGrey.ml10')))
            if import_button.is_displayed():
                import_button.click()
        except (NoSuchElementException, TimeoutException):
            append_log_messages("Import warning not found - proceeding", 0)
            pass

        time.sleep(3)
        
        try:
            WebDriverWait(driver, 3000).until(
            lambda d: d.execute_script(
                "return document.getElementById('PercentageIndicator') === null"
                )
            )
        except NoSuchElementException:
            append_log_messages('Percentage indicator (after creator upload) not found', 1)
            pass
        except TimeoutException:
            append_log_messages('Percentage indicator (after creator upload) timed out', 1)
            pass
        append_log_messages("*** Uploading to Creator is complete ***", 0)
        send_success_email()
        
        driver.quit()
        

    except TimeoutException as e:
        error_message = "Timeout during upload_to_creator function"
        append_log_messages(error_message, 1)
    except NoSuchElementException as e:
        error_message = "Element not found during upload_to_creator: " + str(e)
        append_log_messages(error_message, 1)
    except Exception as e:
        error_message = f"General error in upload_to_creator: {str(e)}"
        append_log_messages(error_message, 1)


#   _____      __      _   ______         _____    _____   _____        _____      __      _    _____   __    __     __    __
#  / ___/     /  \    / ) (_  __ \       / ____\  / ___/  (_   _)      / ___/     /  \    / )  (_   _)  ) )  ( (     \ \  / /
# ( (__      / /\ \  / /    ) ) \ \     ( (___   ( (__      | |       ( (__      / /\ \  / /     | |   ( (    ) )    () \/ ()
#  ) __)     ) ) ) ) ) )   ( (   ) )     \___ \   ) __)     | |        ) __)     ) ) ) ) ) )     | |    ) )  ( (     / _  _ \
# ( (       ( ( ( ( ( (     ) )  ) )         ) ) ( (        | |   __  ( (       ( ( ( ( ( (      | |   ( (    ) )   / / \/ \ \
#  \ \___   / /  \ \/ /    / /__/ /      ___/ /   \ \___  __| |___) )  \ \___   / /  \ \/ /     _| |__  ) \__/ (   /_/      \_\
#   \____\ (_/    \__/    (______/      /____/     \____\ \________/    \____\ (_/    \__/     /_____(  \______/  (/          \)
                                                                                                                               

#--------------------------------------------------------------------------------------------------------


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
                         
                                                                              
def get_csv_files(selected_distributors):
    file_paths = []
    if not os.path.exists(download_dir):
        append_log_messages(f"Directory {download_dir} does not exist!", 1)
        return file_paths

    for entry in os.scandir(download_dir):
        if entry.is_file():
            base_name = entry.name.replace(".csv", "").lower()
            if any(distributor.lower() in base_name for distributor in selected_distributors):
                file_paths.append(entry.path)

    file_paths = list(set(file_paths))
    pandas(file_paths)

    return file_paths


def replace_values(value):
    if value.strip().startswith('X-'):
        value = value.strip()[2:]

    replacements = {
        'RBF36PG': '500002398',
        'RBF30G': '500002388',
        'RBF30WCG': '500002389',
        'RBF36G': '500002400',
        'RBF36WCG': '500002401',
        'RBF36PWCG': '500002399',
        'RBF42G': '500002410',
        'RBF42WCG': '500002411',
        'EVO50': '500002573',
        'EVO60': '500002574',
        'EVO74': '500002608',
        'EVO100': '500002563',
        'BLFPLUGKIT': 'BLF-PLUG-KIT',
        'P45-30AN': 'P45-30A',
        'OLF46-AM': '136786',
        'OLF66-AM': '136793',
        'OLF86-AM': '136809',
        'OLF46': '136786',
        'OLF66': '136793',
        'OLF86': '136809',
        'DHTG32-LP': 'DHTG32-L',
        'DHTG32-NG': 'DHTG32-N'
    }

    for old, new in replacements.items():
        value = value.replace(old, new)
    
    return value

#                         _  .-')
#                        ( \( -O )
#    ,------. .-'),-----. ,------.
# ('-| _.---'( OO'  .-.  '|   /`. '
# (OO|(_\    /   |  | |  ||  /  | |
# /  |  '--. \_) |  |\|  ||  |_.' |
# \_)|  .--'   \ |  | |  ||  .  '.'
#   \|  |_)     `'  '-'  '|  |\  \
#    `--'         `-----' `--' '--'

                                                   
def process_for(df):
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Quantity', 'availabletosell': 'Model'}, inplace=True)
    df.insert(0, 'Distributor', 'FOR')
    df.insert(2, 'Warehouse', '')
    def split_sku(sku):
        sku = str(sku).upper()
        # extracting supplier (first 3 characters) and model (rest of the string after the first dash)
        supplier = sku[:3]
        model = sku[4:] if len(sku) > 4 else ''
        return supplier, model
    df[['Supplier', 'Model']] = df['Model'].apply(lambda x: pd.Series(split_sku(x)))
    df.drop(['sku', 'description'], axis=1, inplace=True)
    
    # sorting columns
    columns = ['Distributor', 'Model', 'Warehouse', 'Quantity', 'Supplier']
    df = df[columns]
    
    df['Model'] = df['Model'].apply(replace_values)
    # filtering for manufacturer/brand
    filter_values_for = {"FMR", "AFD", "AOG", "PET", "DIM"}
    filtered_df = df[df["Supplier"].isin(filter_values_for)].copy()
    
    filtered_df.to_csv(f"{download_dir}/for_formatted.csv", index=False)
    append_log_messages(f"- Formatted FOR.", 0)
    
    return filtered_df


#          _  .-')
#         ( \( -O )
#   ,-.-') ,------.   ,----.
#   |  |OO)|   /`. ' '  .-./-')
#   |  |  \|  /  | | |  |_( O- )
#   |  |(_/|  |_.' | |  | .--, \
#  ,|  |_.'|  .  '.'(|  | '. (_/
# (_|  |   |  |\  \  |  '--'  |
#   `--'   `--' '--'  `------'
  

def process_irg(df):
    df.insert(2, 'Date', current_date_mm_dd_yyyy)
    df.to_csv(f"{download_dir}/irg_formatted.csv", index=False)
    append_log_messages(f"- Formatted IRG.", 0)
    
    return df


#  _  .-')  _   .-')
# ( \( -O )( '.( OO )_
#  ,------. ,--.   ,--.) ,-.-')
#  |   /`. '|   `.'   |  |  |OO)
#  |  /  | ||         |  |  |  \
#  |  |_.' ||  |'.'|  |  |  |(_/
#  |  .  '.'|  |   |  | ,|  |_.'
#  |  |\  \ |  |   |  |(_|  |
#  `--' '--'`--'   `--'  `--'
 
 
def process_rmi(df):
    def map_supplier(pgpgrp):
        supplier_map = {
            "4136": "PET",
            "4137": "PET",
            "4178": "DIM",
            "4740": "AOG",
            "4741": "AOA",
            "4760": "FM",
            "4761": "FMA",
            "4763": "RF",
            "4764": "AFD",
            "4773": "BRO",
            "4785": "PIN"
        }
        return supplier_map.get(pgpgrp, "")
    
    df["PGPGRP"] = df["PGPGRP"].astype('Int64').astype(str).str.strip()
    filter_values_rmi = {"4136", "4178", "4740", "4741", "4760", "4761", "4763", "4764", "4773", "4785", "4137"}
    filtered_df = df[df["PGPGRP"].isin(filter_values_rmi)].copy()
    
    filtered_df.fillna('', inplace=True)
    
    # replacing values with supplier names
    filtered_df["PGPGRP"] = filtered_df["PGPGRP"].apply(map_supplier)
    
    # renaming the first column to 'Supplier'
    filtered_df = filtered_df.rename(columns={filtered_df.columns[0]: 'Supplier'})
    
    # inserting and dropping relevant columns
    filtered_df.insert(0, 'Distributor', 'RMI')
    
    filtered_df.drop(['CTPPGD', 'PGDESC'], axis=1, inplace=True)
    
    # moving columns around and renaming
    if 'PGPRDC' in filtered_df.columns:
        columns = list(filtered_df.columns)
        columns.remove('PGPRDC')
        columns.insert(1, 'PGPRDC')
        filtered_df = filtered_df[columns]
        
        filtered_df.rename(columns={'PGPRDC': 'Model'}, inplace=True)

    if 'TOTAV01' in filtered_df.columns:
        columns = list(filtered_df.columns)
        columns.remove('TOTAV01')
        columns.insert(2, 'TOTAV01')
        filtered_df = filtered_df[columns]
        
        filtered_df.rename(columns={'TOTAV01': 'Quantity'}, inplace=True)
    
    filtered_df.insert(2, 'Warehouse', '')
    
    filtered_df['Model'] = filtered_df['Model'].apply(replace_values)
    
    # saving to new file
    filtered_df.to_csv(f"{download_dir}/rmi_formatted.csv", index=False)
    append_log_messages(f"- Formatted RMI.", 0)
    
    return filtered_df


#  _  .-')              .-') _
# ( \( -O )            (  OO) )
#  ,------. ,--. ,--.  /     '._
#  |   /`. '|  | |  |  |'--...__)
#  |  /  | ||  | | .-')'--.  .--'
#  |  |_.' ||  |_|( OO )  |  |
#  |  .  '.'|  | | `-' /  |  |
#  |  |\  \('  '-'(_.-'   |  |
#  `--' '--' `-----'      `--'
    
    
def process_rut(df):
    valid_suppliers_rut = ["RH Peterson", "Wolf Steel USA, LLC", "Memphis Wood Fire Grills", "AMD Direct"]
    filtered_data = df[df["Supplier Name"].str.strip().isin(valid_suppliers_rut)].copy()

    filtered_data["100"] = filtered_data["Location 100 Available"]
    filtered_data["200"] = filtered_data["Location 200 Available"]
    filtered_data["300"] = filtered_data["Location 300 Available"]

    filtered_data.drop(['Location 100 Available', 'Location 200 Available', 'Location 300 Available'], axis=1, inplace=True)

    # making new rows because its just easier
    new_rows = []
    for _, item in filtered_data.iterrows():
        for index, warehouse in zip([100, 200, 300], ["GA", "FL", "NC"]):
            quantity = int(item[str(index)])
            new_row = {
                'Distributor': 'RUT',
                'Model': item['Item ID'].strip().upper(),
                'Warehouse': warehouse,
                'Quantity': str(quantity),
                'Supplier': ''
            }

            # map supplier
            supplier_name = item['Supplier Name'].strip()
            if supplier_name == "RH Peterson":
                new_row['Supplier'] = "PET"
            elif supplier_name == "Wolf Steel USA, LLC":
                new_row['Supplier'] = "NPL"
            elif supplier_name == "Memphis Wood Fire Grills":
                new_row['Supplier'] = "MEM"
            elif supplier_name == "AMD Direct":
                new_row['Supplier'] = "AMD"

            new_rows.append(new_row)

    # converting to new df
    result_df = pd.DataFrame(new_rows)
    
    result_df['Model'] = result_df['Model'].apply(replace_values)
    
    result_df.to_csv(f"{download_dir}/rut_formatted.csv", index=False)
    append_log_messages(f"- Formatted RUT.", 0)
    
    return result_df


#    ('-.       ('-.    .-')
#   ( OO ).-. _(  OO)  ( OO ).
#   / . --. /(,------.(_)---\_)
#   | \-.  \  |  .---'/    _ |
# .-'-'  |  | |  |    \  :` `.
#  \| |_.'  |(|  '--.  '..`''.)
#   |  .-.  | |  .--' .-._)   \
#   |  | |  | |  `---.\       /
#   `--' `--' `------' `-----'


def process_aes(df):
    
    df.drop(['Inventory Description', 'Inventory ID', 'Warehouse', 'Barcode', 'Supplier', 'Brands', 'Item Class', 'AES Retail Price', 'Volume', 'Weight', 'Item Class Description'], axis=1, inplace=True)
    
    valid_supplier_codes_aes = ["V000064", "V000073", "V000583"]
    filtered_data = df[df["Supplier ID"].str.strip().isin(valid_supplier_codes_aes)].copy()
    filtered_data.insert(0, 'Distributor', 'AES')
    def map_supplier(SupplierID):
        supplier_map = {
            "V000064": "MF",
            "V000073": "PET",
            "V000583": "DIM"
        }
        return supplier_map.get(SupplierID, "")

    # replacing values with supplier names
    filtered_data["Supplier"] = filtered_data["Supplier ID"].apply(map_supplier)

    filtered_data.drop(['Supplier ID'], axis=1, inplace=True)
    
    filtered_data.rename(columns={'Warehouse Location': 'Warehouse', 'Vendor SKU': 'Model', 'Qty. Hard Available': 'Quantity'}, inplace=True)
    
    # stripping 'DC' (Distribution Center)
    filtered_data['Warehouse'] = filtered_data['Warehouse'].replace(' DC', '', regex=True)
    
    # reordering columns
    columns = ['Distributor', 'Model', 'Warehouse', 'Quantity', 'Supplier']
    filtered_data = filtered_data[columns]
    
    filtered_data['Model'] = filtered_data['Model'].apply(replace_values)
    
    filtered_data.to_csv(f"{download_dir}/aes_formatted.csv", index=False)
    append_log_messages(f"- Formatted AES.", 0)
    
    return filtered_data


#  .-') _     .-')   _ .-') _
# (  OO) )   ( OO ).( (  OO) )
# /     '._ (_)---\_)\     .'_
# |'--...__)/    _ | ,`'--..._)
# '--.  .--'\  :` `. |  |  \  '
#    |  |    '..`''.)|  |   ' |
#    |  |   .-._)   \|  |   / :
#    |  |   \       /|  '--'  /
#    `--'    `-----' `-------'
   

def process_tsd(df):
    
    def split_sku(sku):
        sku = str(sku).upper()
        # extracting 3 digit supplier code, and the model (rest of the string after the 3 digit code and the first dash)
        supplier = sku[:3]
        model = sku[4:] if len(sku) > 4 else ''
        return supplier, model
    df[['Supplier', 'Model']] = df['Item ID'].apply(lambda x: pd.Series(split_sku(x)))
    
    # dropping old column
    df.drop(['Item ID'], axis=1, inplace=True)
    
    df.rename(columns={'QTY': 'Quantity'}, inplace=True)
    
    df["Supplier"] = df["Supplier"].astype(str).str.strip()

    # filtering supplier
    valid_supplier_codes_tsd = ["134", "191", "192", "193", "241", "251", "253", "242", "137", "164", "111", "112", "110", "163", "211", "212", "215"]
    filtered_data = df[df["Supplier"].isin(valid_supplier_codes_tsd)].copy()

    supplier_map = {
        "110": "BM",
        "111": "BM",
        "112": "BM",
        "134": "BLZ",
        "137": "MA",
        "164": "WPPO",
        "191": "FM",
        "192": "FM",
        "163": "PRMO",
        "211": "EMP",
        "212": "EMP",
        "215": "EMP",
        "193": "AOG",
        "241": "RF",
        "242": "AFD",
        "251": "MF",
        "253": "DIM"
    }

    # mapping supplier
    filtered_data["Supplier"] = filtered_data["Supplier"].map(supplier_map)

    # inserting necessary columns and then reordering and sasving
    filtered_data.insert(0, 'Distributor', 'TSD')
    filtered_data.insert(2, 'Warehouse', '')
    columns = ['Distributor', 'Model', 'Warehouse', 'Quantity', 'Supplier']
    filtered_data = filtered_data[columns]
    
    filtered_data['Model'] = filtered_data['Model'].apply(replace_values)
    
    filtered_data.to_csv(f"{download_dir}/tsd_formatted.csv", index=False)
    append_log_messages(f"- Formatted TSD.", 0)
    
    return filtered_data


#    ('-.       .-') _
#   ( OO ).-.  (  OO) )
#   / . --. /,(_)----.   ,------.
#   | \-.  \ |       |('-| _.---'
# .-'-'  |  |'--.   / (OO|(_\
#  \| |_.'  |(_/   /  /  |  '--.
#   |  .-.  | /   /___\_)|  .--'
#   |  | |  ||        | \|  |_)
#   `--' `--'`--------'  `--'


def process_azf(df):
    filter_suppliers = ["Bromic", "LeGriddle", "Dimplex", "Modern Flames", "Empire", "Primo", "STOLL", "Modern Home Products",  "American Made Grill", "American Fyre Design", "Firemagic", "American Outdoor Grill", "Real Fyre", "Napoleon Hearth"]
    
    filtered_data = df[df["Preferred Vendor"].isin(filter_suppliers)].copy()
    
    filtered_data.drop(['Supplier SKU', 'Your Cost', '% Discount', 'MSRP', 'Supplier Description', 'Avg Lead Time', 'Next Available Date', 'Next Available Quantity'], axis=1, inplace=True)
    
    supplier_codes_azf = {
        "Bromic": "BRO",
        "LeGriddle": "LGR",
        "Dimplex": "DIM",
        "Modern Flames": "MF",
        "American Fyre Design": "AFD",
        "Firemagic": "FM",
        "American Outdoor Grill": "AOG",
        "Real Fyre": "RF",
        "Napoleon Hearth": "NPL",
        "Primo": "PRMO",
        "Empire": "EMP",
        "Modern Home Products": "MHP",
        "American Made Grill": "AMG",
        "STOLL": "STOL"
    }
    
    new_rows = []

    for _, item in filtered_data.iterrows():
        for stock_column, warehouse in zip(["AZ In Stock", "TX In Stock"], ["AZ", "TX"]):
            # getting warehouse qty
            stock_value = item.get(stock_column, '')
            stock_value = str(stock_value).strip() if isinstance(stock_value, str) else stock_value

            try:
                quantity = int(stock_value)
            except (ValueError, TypeError):
                quantity = 0

            # creating a new row object
            new_row = {
                'Distributor': 'AZF',
                'Model': item['Manufacturer SKU'].strip().upper() if item['Manufacturer SKU'] else "",
                'Warehouse': warehouse,
                'Quantity': str(quantity),
                'Supplier': ''
            }

            # mapping supplier
            preferred_vendor = item['Preferred Vendor'].strip() if item['Preferred Vendor'] else ""
            new_row['Supplier'] = supplier_codes_azf.get(preferred_vendor, "")

            new_rows.append(new_row)

    # converting to new df
    result_df = pd.DataFrame(new_rows)
    
    result_df['Model'] = result_df['Model'].apply(replace_values)
    
    append_log_messages("- Formatted AZF.", 0)
    result_df.to_csv(f"{download_dir}/azf_formatted.csv", index=False)
    
    return result_df


#    _ (`-.              .-') _
#   ( (OO  )            ( OO ) )
#  _.`     \ ,-.-') ,--./ ,--,'
# (__...--'' |  |OO)|   \ |  |\
#  |  /  | | |  |  \|    \|  | )
#  |  |_.' | |  |(_/|  .     |/
#  |  .___.',|  |_.'|  |\    |
#  |  |    (_|  |   |  | \   |
#  `--'      `--'   `--'  `--'
 
 
def process_pin(df):
    df.fillna('', inplace=True)
    
    filter_suppliers_pin = ["Memphis", "Coyote", "VentAHood", "RTAOutdoor"]
    filtered_data = df[df["Product Group"].isin(filter_suppliers_pin)].copy()
    
    supplier_map_pin = {
        "Coyote": "COY",
        "VentAHood": "VAH",
        "Memphis": "MEM",
        "RTAOutdoor": "COY",
    }

    # mapping supplier
    filtered_data["Supplier"] = filtered_data["Product Group"].map(supplier_map_pin)
    
    filtered_data.drop(['Product Type', 'Description', 'Product Group'], axis=1, inplace=True)
    filtered_data.rename(columns={'Product Number': 'Model', 'Stock Status': 'Quantity'}, inplace=True)
    
    filtered_data.insert(0, 'Distributor', 'PIN')
    filtered_data.insert(2, 'Warehouse', '')
    columns = ['Distributor', 'Model', 'Warehouse', 'Quantity', 'Supplier']
    filtered_data = filtered_data[columns]
    
    filtered_data['Model'] = filtered_data['Model'].apply(replace_values)
     
    append_log_messages("- Formatted PIN.", 0)
    filtered_data.to_csv(f"{download_dir}/pin_formatted.csv", index=False)
    
    return filtered_data


#    _ (`-.            .-')
#   ( (OO  )          ( OO ).
#  _.`     \  .-----.(_)---\_)
# (__...--'' '  .--.//    _ |
#  |  /  | | |  |('-.\  :` `.
#  |  |_.' |/_) |OO  )'..`''.)
#  |  .___.'||  |`-'|.-._)   \
#  |  |    (_'  '--'\\       /
#  `--'       `-----' `-----'


def process_pcs(df):

    df.drop(['LG', 'Description'], axis=1, inplace=True)
    df.rename(columns={'Part #': 'Model'}, inplace=True)
    
    df.insert(0, 'Distributor', 'PCS')
    df.insert(2, 'Warehouse', '')
    df.insert(3, 'Supplier', 'Not Specified')
    columns = ['Distributor', 'Model', 'Warehouse', 'Quantity', 'Supplier']
    df = df[columns]
    
    df['Model'] = df['Model'].apply(replace_values)
    
    append_log_messages("- Formatted PCS.", 0)
    df.to_csv(f"{download_dir}/pcs_formatted.csv", index=False)

    return df


def process_default(df):
    append_log_messages("process_default hit on process_file function", 1)
    return



def process_file(file_path):
    base_name = os.path.basename(file_path).replace(".csv", "").lower()
    switch = {
        'aes': process_aes,
        'azf': process_azf,
        'for': process_for,
        'pcs': process_pcs,
        'pin': process_pin,
        'rmi': process_rmi,
        'rut': process_rut,
        'tsd': process_tsd,
        'irg': process_irg
    }

    with print_lock:
        try:
            print(f"Processing file: {file_path}")
            # these "if's" are fixing issues with the spreadsheets. when adding a new distributor,
            # you likely will not need to modify anything, but you can here if needed
            if '_formatted' not in base_name.lower():
                if 'azf' in base_name:
                    df = pd.read_csv(file_path)
                    if 'Warning: Null value is eliminated by an aggregate or other SET operation' in df.columns[0]:
                        df = pd.read_csv(file_path, skiprows=1, header=0)
                        headers = ['Preferred Vendor', 'Supplier SKU', 'Manufacturer SKU', 'AZ In Stock',
                                'TX In Stock', 'Your Cost', '% Discount', 'MSRP', 'Supplier Description',
                                'Avg Lead Time', 'Next Available Date', 'Next Available Quantity']
                        df.columns = headers
                    else:
                        df = pd.read_csv(file_path)
                elif 'pin' in base_name:
                    try:
                        df = pd.read_csv(file_path, encoding='utf-8')
                    except UnicodeDecodeError:
                        df = pd.read_csv(file_path, encoding='ISO-8859-1')
                else:
                    # default scenario
                    df = pd.read_csv(file_path)

                process_func = switch.get(base_name, process_default)
                processed_df = process_func(df)

                if 'irg' not in base_name:
                    formatted_dfs_queue.put(processed_df)

        except Exception as e:
            append_log_messages(f"Error processing {file_path}: {str(e)}", 1)


def pandas(file_paths):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_file, file_path): file_path for file_path in file_paths}

        for future in as_completed(futures):
            file_path = futures[future]
            try:
                future.result()
            except Exception as e:
                append_log_messages(f"Error in processing file {file_path}: {str(e)}", 1)

    formatted_dfs = []
    while not formatted_dfs_queue.empty():
        formatted_dfs.append(formatted_dfs_queue.get())

    # combining into master file
    if formatted_dfs:
        master_df = pd.concat(formatted_dfs, ignore_index=True)
        
        # converting to integer
        master_df['Quantity'] = pd.to_numeric(master_df['Quantity'], errors='coerce').fillna(0).astype(int)
        
        # replacing all negatives with 0
        master_df['Quantity'] = master_df['Quantity'].apply(lambda x: 0 if x < 0 else x)
      
        # inserting unix epoch
        master_df.insert(4, 'Date', int(time.time()))
        columns = ['Distributor', 'Model', 'Warehouse', 'Quantity', 'Supplier', 'Date']
        master_df = master_df[columns]
        
        master_row_count = master_df.shape[0]
        
        master_df.to_csv(master_file_path, index=False)
        append_log_messages(f"- MASTER FILE CREATED AT {master_file_path}", 0)
    else:
        append_log_messages("No formatted files found for the master file.", 1)
        
        
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


#  /$$      /$$ /$$     /$$ /$$$$$$   /$$$$$$  /$$             /$$$$$$ /$$      /$$ /$$$$$$$   /$$$$$$  /$$$$$$$  /$$$$$$$$
# | $$$    /$$$|  $$   /$$//$$__  $$ /$$__  $$| $$            |_  $$_/| $$$    /$$$| $$__  $$ /$$__  $$| $$__  $$|__  $$__/
# | $$$$  /$$$$ \  $$ /$$/| $$  \__/| $$  \ $$| $$              | $$  | $$$$  /$$$$| $$  \ $$| $$  \ $$| $$  \ $$   | $$
# | $$ $$/$$ $$  \  $$$$/ |  $$$$$$ | $$  | $$| $$              | $$  | $$ $$/$$ $$| $$$$$$$/| $$  | $$| $$$$$$$/   | $$
# | $$  $$$| $$   \  $$/   \____  $$| $$  | $$| $$              | $$  | $$  $$$| $$| $$____/ | $$  | $$| $$__  $$   | $$
# | $$\  $ | $$    | $$    /$$  \ $$| $$/$$ $$| $$              | $$  | $$\  $ | $$| $$      | $$  | $$| $$  \ $$   | $$
# | $$ \/  | $$    | $$   |  $$$$$$/|  $$$$$$/| $$$$$$$$       /$$$$$$| $$ \/  | $$| $$      |  $$$$$$/| $$  | $$   | $$
# |__/     |__/    |__/    \______/  \____ $$$|________/      |______/|__/     |__/|__/       \______/ |__/  |__/   |__/
#                                         \__/
                                                                                                                         
def import_csv_to_mysql(mysql_table, file_to_upload):
    conn = None
    cursor = None
    try:
        df = pd.read_csv(file_to_upload)
        df.fillna(0, inplace=True)

        # fixing data
        if 'Quantity' in df.columns:
            df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
            df['Quantity'].fillna(0, inplace=True)
            df['Quantity'] = df['Quantity'].astype(int)

        conn = mysql.connector.connect(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_db)
        
        if conn.is_connected():
            append_log_messages("Connected to the database. Running queries...", 0)
            cursor = conn.cursor()
            create_table_query = "CREATE TABLE IF NOT EXISTS " + mysql_table + " ("
            create_table_query += ", ".join([f"{col} VARCHAR(255)" for col in df.columns])
            create_table_query += ")"
            cursor.execute(create_table_query)
            cursor.execute(f"TRUNCATE TABLE {mysql_table}")
            append_log_messages(f"Table {mysql_table} truncated.", 0)

            total_rows_inserted = 0
            for i, row in df.iterrows():
                insert_query = f"INSERT INTO {mysql_table} ({', '.join(df.columns)}) VALUES ({', '.join(['%s']*len(df.columns))})"
                cursor.execute(insert_query, tuple(row))
                total_rows_inserted += cursor.rowcount

            conn.commit()
            append_log_messages(f"Data imported to {mysql_table} successfully! Total rows imported: {total_rows_inserted}", 0)

    except Error as e:
        append_log_messages(f"An error occurred while uploading to {mysql_table}: {e}", 1)
        if conn:
            conn.rollback()

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())