from flask import Flask, redirect
from database import Database
import json

app = Flask(__name__)

db = Database("config.json")
db.create_tables()


@app.route("/health", methods=["GET"])
def health():
    status = db.health_check()
    print("Health check: ", status)

    return {"status": status}


@app.route("/track/<unique_id>", methods=["GET"])
def track_email(unique_id):

    print("Tracking email with unique_id: ", unique_id)
    tracking_satus = db.track_email(unique_id)

    print("Tracking status: ", tracking_satus)

    with open("config.json") as cfg_file:
        cfg = json.load(cfg_file)

        # return redirect(
        #     cfg.get("conkart_logo"),
        #     code=302,
        # )
        
        return cfg.get("conkart_logo")


""" if __name__ == "__main__":
    app.run(debug=True) """
