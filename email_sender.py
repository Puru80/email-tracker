import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr, formatdate
import random, time, json, argparse, uuid
import pandas as pd
from database import Database

db = Database("config.json")
db.create_tables()


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
        server_url,
    ):
        self.company_name = company_name
        self.contact = contact
        self.display_name = display_name
        self.email_address = email_address
        self.app_password = app_password
        self.smtp_server = smtp_server
        self.cc = cc
        self.server_url = server_url

        # Email content
        self.subject = "Partnership Opportunity for Bill Discounting!"
        self.body = f"""
Dear Sir/Madam,<br>
<br>
Warm Greetings.<br>
<br>
We are Conkart, a dedicated B2B and B2C marketplace catering to the construction industry. We are looking to partner with NBFCs interested in providing real-time bill discounting services for construction material transactions on our platform.<br>
<br>
Our marketplace serves a diverse clientele, including:<br>
<br>
Developers / Builders / Private or Government Contractor<br>
One-Time Buyers (B2C) – Individuals constructing or renovating their homes. In this industry, bill discounting is typically extended for periods ranging from 7 to 60 days, and we see a significant opportunity to streamline this process for our buyers and sellers. Your participation as a financial partner would enable seamless, efficient transactions and enhance liquidity for our customers.<br>
<br>
If you currently offer such services or are willing to explore this opportunity, we would be delighted to discuss how we can collaborate. Please reply to this email, and we can arrange a meeting to discuss further details.<br>
<br>
Looking forward to your positive response.<br>
<br>
Warm regards,<br>
{company_name}<br>
{cc}<br>
{contact}
        """

        # Set up the SMTP server
        self.server = smtplib.SMTP_SSL(smtp_server)
        self.server.ehlo()
        self.server.login(email_address, app_password)

    def quit(self):
        self.server.quit()


email_recepients = ['puru.agar99@gmail.com']


def get_email_recepients():
    file = pd.ExcelFile(r"D:\aargo\Nbfc .xlsx")
    df = file.parse("Sheet 16")

    file.close()

    df.columns = ["ID", "Name", "Location", "D", "E", "F", "G", "H", "Email"]
    list = df["Email"].tolist()

    email_recepients = []

    for str in list:
        if str == "-NA-" or str == "" or str == "nan":
            # print("Skipping nan: ", str)
            continue
        arr = str.split(";")

        for email in arr:
            if len(email.strip()) > 0:
                email_recepients.append(email.strip().lower())

    return email_recepients


def send_email(recipient, config: Config):

    msg = MIMEMultipart('alternative')

    unique_id = str(uuid.uuid4())
    tracking_pixel_url = f"{config.server_url}/track/{unique_id}"

    email_content = f"""
    
<html>
  <body>
    <p>{config.body}</p>
    <img src="{tracking_pixel_url}" alt="Conkart Logo" width="150x" height="50px">
  </body>
</html>

"""
    msg.attach(MIMEText(email_content, "html"))
    msg["Subject"] = config.subject
    msg["From"] = formataddr((config.display_name, config.email_address))
    msg["Cc"] = config.cc
    msg["To"] = recipient
    msg["Reply-To"] = config.cc
    mail_time = formatdate(timeval=None, localtime=True)
    print("Mail time: ", mail_time)
    msg["Date"] = mail_time

    print("Sending email to: ", recipient)

    try:
        error = config.server.sendmail(config.email_address, recipient, msg.as_string())
        print("Error: ", error)

        db.register_email(recipient, unique_id)
    except Exception as e:
        print(e)
        print("failed to send mail: ", recipient)


def main(config):

    # email_recepients = get_email_recepients()
    print("Email recepients: ", len(email_recepients))
    # print("Email recepients: ", email_recepients)
    # return
    
    """ final_email_recepients = []
    to_send_email = False
    for email in email_recepients:
        if email == "some@example.com":
            to_send_email = True
            # continue

        if to_send_email:
            final_email_recepients.append(email) """

    for recipient in email_recepients:
        send_email(recipient, config)

        random_time = random.randint(1, 10)
        time.sleep(random_time)

    db.close()
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
            cfg.get("server_url"),
        )

        main(email_config)
