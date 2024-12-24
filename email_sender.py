import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import random, time, json, argparse
import pandas as pd


class Config:

    def __init__(
        self,
        company_name,
        contact,
        display_name,
        email_address,
        app_password,
        smtp_server,
        cc,
    ):
        self.company_name = company_name
        self.contact = contact
        self.display_name = display_name
        self.email_address = email_address
        self.app_password = app_password
        self.smtp_server = smtp_server
        self.cc = cc

        # Email content
        self.subject = "Partnership Opportunity for Bill Discounting!"
        self.body = f"""
        Dear Sir/Madam,

        Warm Greetings.

        We are Conkart, a dedicated B2B and B2C marketplace catering to the construction industry. We are looking to partner with NBFCs interested in providing real-time bill discounting services for construction material transactions on our platform.

        Our marketplace serves a diverse clientele, including:

        Developers / Builders / Private or Government Contractor
        One-Time Buyers (B2C) – Individuals constructing or renovating their homes. In this industry, bill discounting is typically extended for periods ranging from 7 to 60 days, and we see a significant opportunity to streamline this process for our buyers and sellers. Your participation as a financial partner would enable seamless, efficient transactions and enhance liquidity for our customers.

        If you currently offer such services or are willing to explore this opportunity, we would be delighted to discuss how we can collaborate. Please reply to this email, and we can arrange a meeting to discuss further details.

        Looking forward to your positive response.

        Warm regards,
        {company_name}
        {contact}
        """

        # Set up the SMTP server
        self.server = smtplib.SMTP_SSL(smtp_server)
        self.server.ehlo()
        self.server.login(email_address, app_password)

    def quit(self):
        self.server.quit()


def get_email_recepients():
    file = pd.ExcelFile(r"D:\aargo\Nbfc .xlsx")
    df = file.parse("Sheet 24")

    df.columns = ["ID", "Name", "Location", "D", "E", "F", "G", "H", "Email"]

    return df["Email"].tolist()


# TODO: Add id to the email body
def send_email(recipient, config: Config):

    msg = MIMEText(config.body)
    msg["Subject"] = config.subject
    msg["From"] = formataddr((config.display_name, config.email_address))
    msg["cc"] = config.cc
    msg["To"] = recipient

    try:
        config.server.sendmail(config.email_address, recipient, msg.as_string())
    except:
        print("failed to send mail: ", recipient)

""" 
@app.route("/send_email", methods=["POST"])
def send_email():
    data = request.get_json()
    recipient_email = data["recipient_email"]
    email_subject = data["email_subject"]
    email_body = data["email_body"]

    unique_id = str(uuid.uuid4())
    tracking_pixel_url = f"http://localhost:5000/track?id={unique_id}"

    email_content = f"""
    
    # <html>
    #   <body>
    #     <p>{email_body}</p>
    #     <img src="{tracking_pixel_url}">
    #   </body>
    # </html>
    
"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = email_subject
    msg["From"] = email_address
    msg["To"] = recipient_email
    msg.attach(MIMEText(email_content, "html"))

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(email_address, email_password)
    server.sendmail(email_address, recipient_email, msg.as_string())
    server.quit()

    return jsonify({"message": "Email sent successfully"})
"""

def main(config):

    email_recepients = get_email_recepients()

    for recipient in email_recepients:
        send_email(recipient)

        random_time = random.randint(1, 10)
        time.sleep(random_time)

    config.quit()


def set_parser() -> argparse.ArgumentParser:
    """
    Sets up the argument parser for command-line input.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="config file name.", default="config.json"
    )
    return parser


if __name__ == "__main__":

    parser = set_parser()
    args = parser.parse_args()

    with open(args.config, "r") as cfg_file:
        cfg = json.load(cfg_file)
        # environment = cfg.get("environment", "prod")

        email_params = cfg.get("prod_email_config", {})

        email_config = Config(
            email_params.get("company_name"),
            email_params.get("contact"),
            email_params.get("display_name"),
            email_params.get("email_address"),
            email_params.get("email_password"),
            email_params.get("smtp_server"),
            email_params.get("cc"),
        )

        main(email_config)
