import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QPushButton, QLabel, QVBoxLayout, QWidget, QCheckBox, QHBoxLayout
from PyQt6.QtCore import Qt
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
import time
import threading
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import csv
import json
from urllib.parse import quote, urlencode
import requests
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
zoho_mail_account_id = os.getenv("ZOHO_MAIL_zoho_mail_account_id")
zoho_mail_folder_id = os.getenv("ZOHO_MAIL_FOLDER_ID")

# other
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
        

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("IRG Distributor Spreadsheets")
        self.setGeometry(100, 100, 600, 500)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        self.label = QLabel("Test", self)
        main_layout.addWidget(self.label)

        button_layout = QHBoxLayout()
        self.downloadSelectedButton = QPushButton("Download Selected", self)
        self.downloadSelectedAndUploadToCreatorButton = QPushButton("Downloaded Select and Upload to Creator", self)
        self.uploadToCreatorButton = QPushButton("Upload to Creator", self)
        button_layout.addWidget(self.downloadSelectedButton)
        button_layout.addWidget(self.downloadSelectedAndUploadToCreatorButton)
        button_layout.addWidget(self.uploadToCreatorButton)
        main_layout.addLayout(button_layout)

        checkbox_layout = QHBoxLayout()
        self.checkboxes = []
        checkbox_labels = ["AES", "AZF", "FOR", "RMI", "RUT", "TSD", "PIN", "PCS"]
        for label in checkbox_labels:
            checkbox = QCheckBox(label, self)
            checkbox.setChecked(True)
            checkbox.stateChanged.connect(self.update_label)
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
            }

            QPushButton {
                background-color: #000;
                border-radius: 10px;
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

        self.update_label()

    def update_label(self):
        checked_labels = [checkbox.text() for checkbox in self.checkboxes if checkbox.isChecked()]
        distString = ', '.join(checked_labels) if checked_labels else "No selection"
        self.label.setText(f"Selected: {distString}")

    def downloadSelected(self):
        self.label.setText("Download selected was clicked!")
        self.getAccessTokenFromRefreshToken()

    def downloadSelectedAndUploadToCreator(self):
        self.label.setText("Download selected and uploaded to creator was clicked!")

    def uploadToCreator(self):
        self.label.setText("Upload to creator was clicked!")

    def getAccessTokenFromRefreshToken(self):
        try:
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
                global access_token
                access_token = data.get("access_token")
                
        except Exception as e:
            print("Error getting access token:", e)
            send_error_email("Error getting access token")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())