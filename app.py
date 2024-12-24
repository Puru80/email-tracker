from flask import Flask, request, jsonify
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import uuid
import sqlite3

app = Flask(__name__)

# Email configuration
email_address = "your_email@gmail.com"
email_password = "your_email_password"
smtp_server = "smtp.gmail.com"
smtp_port = 587

# Database configuration
conn = sqlite3.connect("tracking_data.db")
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS tracking_data
    (id INTEGER PRIMARY KEY, unique_id TEXT)
"""
)
conn.commit()


@app.route("/track", methods=["GET"])
def track_email():
    unique_id = request.args.get("id")
    cursor.execute("SELECT * FROM tracking_data WHERE unique_id=?", (unique_id,))
    if cursor.fetchone():
        return "Email already tracked"
    else:
        cursor.execute("INSERT INTO tracking_data (unique_id) VALUES (?)", (unique_id,))
        conn.commit()
        return "Email tracked successfully"


if __name__ == "__main__":
    app.run(debug=True)
