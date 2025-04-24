from email.mime.multipart import MIMEMultipart
from email.utils import formataddr, formatdate
import smtplib
import time, json, argparse, uuid
from email.mime.text import MIMEText
import threading
from queue import Queue
from database import Database
from datetime import datetime
import pytz


db = Database("config_new.json")
db.create_tables()


class Config:

    def __init__(
        self,
        company_name,
        contact,
        display_name,
        email_address,
        smtp_username,
        smtp_password,
        smtp_server,
        cc,
        server_url,
    ):
        self.company_name = company_name
        self.contact = contact
        self.display_name = display_name
        self.email_address = email_address
        self.smtp_username = smtp_username
        self.smtp_password = smtp_password
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
        """

        # Set up the SMTP server
        self.server = smtplib.SMTP_SSL(smtp_server)
        self.server.ehlo()
        self.server.login(smtp_username, smtp_password)

    def quit(self):
        self.server.quit()


class EmailSender:
    def __init__(self, smtp_server, smtp_port, smtp_username, smtp_password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_username = smtp_username
        self.password = smtp_password
        self.queue = Queue()
        self.rate_limit = 14  # emails per second
        self.lock = threading.Lock()
        self.email_records = []

    def get_current_time_string(self):
        current_time = datetime.now(pytz.utc)
        time_string = current_time.strftime("%Y-%m-%d %H:%M:%S.%f") + " +00:00"

        return time_string

    def send_email(
        self, from_email, to_email, display_name, cc, subject, body, server_url
    ):
        msg = MIMEMultipart("alternative")

        unique_id = str(uuid.uuid4())
        tracking_pixel_url = f"{server_url}/track/{unique_id}"

        email_content = f"""
    
<html>
  <body>
    <p>{body}</p>
    <div style="height: 60px; width: 150px; display: block; background: url({tracking_pixel_url}); background-size: contain;"></div>
  </body>
</html>

"""

        msg.attach(MIMEText(email_content, "html"))
        msg["Subject"] = subject
        msg["From"] = formataddr((display_name, from_email))
        msg["Cc"] = cc
        msg["To"] = to_email
        msg["Reply-To"] = cc
        mail_time = formatdate(timeval=None, localtime=True)
        msg["Date"] = mail_time

        with self.lock:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.smtp_username, self.password)
            server.sendmail(from_email, to_email, msg.as_string())
            server.quit()

            self.email_records.append(
                {
                    "to_email": to_email,
                    "unique_id": unique_id,
                    "sent_at": self.get_current_time_string(),
                }
            )

            # Perform bulk insert every 100 records
            if len(self.email_records) >= 2:
                db.bulk_register_emails(self.email_records)
                self.email_records = self.email_records[3:]
                print(self.email_records)

    def worker(self):
        while True:
            from_email, to_email, display_name, cc, subject, body, server_url = (
                self.queue.get()
            )
            start_time = time.time()
            self.send_email(
                from_email, to_email, display_name, cc, subject, body, server_url
            )
            elapsed_time = time.time() - start_time
            delay = max(0, 1 / self.rate_limit - elapsed_time)
            time.sleep(delay)
            self.queue.task_done()

    def start(self, num_workers):
        threads = []
        for _ in range(num_workers):
            thread = threading.Thread(target=self.worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)

    def add_email(
        self, from_email, to_email, display_name, cc, subject, body, server_url
    ):
        self.queue.put(
            (from_email, to_email, display_name, cc, subject, body, server_url)
        )

    def wait(self):
        self.queue.join()


""" def send_emails_with_rate_limit(
    emails, rate_per_second, smtp_server, smtp_port, smtp_username, smtp_password
):
    email_queue = Queue()
    num_senders = 5  # Adjust the number of sender threads as needed
    senders = []

    for _ in range(num_senders):
        sender = EmailSender(
            email_queue, smtp_server, smtp_port, smtp_username, smtp_password
        )
        senders.append(sender)
        sender.daemon = True
        sender.start()

    start_time = time.time()
    emails_sent = 0

    for email in emails:
        email_queue.put(email)
        emails_sent += 1
        elapsed_time = time.time() - start_time
        if emails_sent < rate_per_second:
            time.sleep(1.0 / rate_per_second)  # Adjust sleep time for desired rate
        else:
            time.sleep(1.0)
            start_time = time.time()
            emails_sent = 0

    for _ in range(num_senders):
        email_queue.put(None)

    for sender in senders:
        sender.join() """


def main(email_config: Config):
    # Example email data
    """
    Get paginated company details from database
    and send emails to each company.
    """

    email_sender = EmailSender(
        email_config.smtp_server,
        587,  # SMTP port for TLS
        email_config.smtp_username,
        email_config.smtp_password,
    )
    email_sender.start(num_workers=10)

    page = 1
    page_size = 500
    offset = 0

    email_sender.add_email(
        email_config.email_address,
        "puru.agar99@gmail.com",
        email_config.display_name,
        email_config.cc,
        email_config.subject,
        email_config.body,
        email_config.server_url,
    )
    email_sender.add_email(
        email_config.email_address,
        "abhishekchavda14@gmail.com",
        email_config.display_name,
        email_config.cc,
        email_config.subject,
        email_config.body,
        email_config.server_url,
    )
    email_sender.add_email(
        email_config.email_address,
        "gaganagarwala@gmail.com",
        email_config.display_name,
        email_config.cc,
        email_config.subject,
        email_config.body,
        email_config.server_url,
    )

    """ while True:
        # Get company details from database
        company_details = db.get_company_details(offset=offset)
        print("Company Details: ", len(company_details))
        if not company_details:
            break

        # Send emails to each company
        for company in company_details:
            arr = company_details[1].split(";")
            for email in arr:
                email_sender.add_email(
                    email_config.email_address,
                    email.strip(),
                    email_config.display_name,
                    email_config.cc,
                    email_config.subject,
                    email_config.body,
                    email_config.server_url,
                )

        offset += page_size
        page += 1 """

    email_sender.wait()

    db.close()
    email_config.quit()

    return


def set_parser() -> argparse.ArgumentParser:
    """
    Sets up the argument parser for command-line input.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="config file name.", default="config_new.json"
    )
    return parser


if __name__ == "__main__":

    parser = set_parser()
    args = parser.parse_args()
    with open(args.config, "r") as cfg_file:
        cfg = json.load(cfg_file)
        # environment = cfg.get("environment", "prod")

        email_params = cfg.get("email_config", {})
        aws_smtp_config = cfg.get("aws_smtp_config", {})

        email_config = Config(
            email_params.get("company_name"),
            email_params.get("contact"),
            email_params.get("display_name"),
            email_params.get("email_address"),
            aws_smtp_config.get("smtp_username"),
            aws_smtp_config.get("smtp_password"),
            aws_smtp_config.get("smtp_server"),
            email_params.get("cc"),
            cfg.get("server_url"),
        )

    main(email_config)
