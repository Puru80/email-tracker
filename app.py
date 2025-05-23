from flask import Flask, Response, send_file
from database import Database
import requests, json

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

        image_url = cfg.get("conkart_logo")

        image_response = requests.get(image_url, stream=True)
        if image_response.status_code != 200:
            return "Image not found", 404

        return Response(
            image_response.content,
            mimetype="image/png",
            headers={"Content-Disposition": "inline"}
        )


if __name__ == "__main__":
    app.run(debug=True)
