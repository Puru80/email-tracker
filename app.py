from flask import Flask
from database import Database
from utils import Utils

app = Flask(__name__)

db = Database("config.json")
db.create_tables()


@app.route("/health", methods=["GET"])
def health():
    print("Health check: ", db.health_check())

    return "Healthy"


@app.route("/track/<unique_id>", methods=["GET"])
def track_email(unique_id):

    print("Tracking email with unique_id: ", unique_id)
    tracking_satus = db.track_email(unique_id)

    print("Tracking status: ", tracking_satus)
    return tracking_satus


""" if __name__ == "__main__":
    app.run(debug=True)
 """