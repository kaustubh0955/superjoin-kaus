import pandas as pd
import json
import requests
import psycopg2
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from celery import Celery
import redis

app = Flask(__name__)

# Redis setup
r = redis.Redis(host='localhost', port=6379, db=0)

# Setup Celery for async tasks
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.result_backend = app.config['CELERY_RESULT_BACKEND']

# PostgreSQL database connection
db_conn = psycopg2.connect(
    dbname='superjoin',
    user='postgres',
    password='Enter your password',
    host='localhost',
    port=5432,
    sslmode='disable'
)

# Google Sheets details
GOOGLE_SHEET_ID = '175uLPtnOr6D-LmiuWcNLIPc_MBQcFKqdoJ0uSeb7w20'  
GOOGLE_SHEET_RANGE = 'Sheet1' 
SERVICE_ACCOUNT_FILE = 'path/to/credentials.json'  # Using credentials.json file

# Google Sheets API setup
def get_google_sheets_service():
    credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    return credentials

# Function to fetch all tables and their data from PostgreSQL
def fetch_all_data_from_db():
    cursor = db_conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    tables = cursor.fetchall()
    
    all_data = {}
    for (table_name,) in tables:
        cursor.execute(f"SELECT * FROM {table_name};")
        all_data[table_name] = cursor.fetchall()

    cursor.close()
    return all_data

# Function to fetch all data from Google Sheets
def fetch_all_data_from_google_sheets():
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/{GOOGLE_SHEET_RANGE}"
    credentials = get_google_sheets_service()
    access_token = credentials.token

    headers = {
        'Authorization': f'Bearer {access_token}',
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.text}")

    return response.json().get('values', [])

# Function to sync data from DB to Google Sheets
@celery.task
def sync_data_to_google_sheets(db_data):
    # Convert DB data to format suitable for Google Sheets
    sheet_data = []
    for table_name, rows in db_data.items():
        for row in rows:
            sheet_data.append(row)  # Adjust this based on your sheet structure

    # Send the data to Google Sheets (updating it)
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/{GOOGLE_SHEET_RANGE}?valueInputOption=USER_ENTERED"
    credentials = get_google_sheets_service()
    access_token = credentials.token

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    body = {
        'values': sheet_data
    }

    response = requests.put(url, headers=headers, json=body)
    if response.status_code != 200:
        raise Exception(f"Failed to update Google Sheets: {response.text}")

# Sync endpoint to sync all data between DB and Google Sheets
@app.route('/sync_all', methods=['POST'])
def sync_all():
    try:
        # Fetch all data from the database
        db_data = fetch_all_data_from_db()
        
        # Enqueue the sync task to Google Sheets
        sync_data_to_google_sheets.delay(db_data)

        return jsonify({"status": "sync initiated"}), 202
    except Exception as e:
        print(f"Error during sync: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# Entry point to run the Flask app
if __name__ == "__main__":
    app.run(debug=True)