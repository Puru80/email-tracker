from email.mime.multipart import MIMEMultipart
from email.utils import formataddr, formatdate
import smtplib
import time, json, argparse, uuid
from email.mime.text import MIMEText
import threading
from queue import Queue
from database import Database
from datetime import datetime
import pytz, logging

# Logger setup
logger = logging.getLogger("email_sender")
logger.setLevel(logging.INFO)

# Create a file handler
file_handler = logging.FileHandler("email_sender.log")
file_handler.setLevel(logging.INFO)

# Create a formatter and set it for the file handler
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

db = Database("config_new.json")
db.create_tables()


class Config:

    def __init__(
        self,
        company_name,
        contact,
        display_name,
        email_address,
        reply_to,
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
        self.reply_to = reply_to
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


class EmailSender:
    def __init__(self, smtp_server_url, smtp_port, smtp_username, smtp_password):
        self.smtp_server_url = smtp_server_url
        self.smtp_port = smtp_port
        self.smtp_username = smtp_username
        self.password = smtp_password
        self.queue = Queue()
        self.emails_sent = set()
        self.rate_limit = 14  # emails per second
        self.lock = threading.Lock()
        self.email_records = []
        self.emails_sent_in_last_second = 0
        self.last_second = int(time.time())
        self.rate_limit_lock = threading.Lock()
        self.smtp_server = None

    def connect_to_smtp_server(self):
        smtp_server = smtplib.SMTP(self.smtp_server_url, self.smtp_port)
        smtp_server.starttls()
        smtp_server.login(self.smtp_username, self.password)
        return smtp_server

    def get_current_time_string(self):
        current_time = datetime.now(pytz.utc)
        time_string = current_time.strftime("%Y-%m-%d %H:%M:%S.%f") + " +00:00"

        return time_string

    def increment_emails_sent(self):
        with self.rate_limit_lock:
            current_second = int(time.time())
            if current_second != self.last_second:
                self.last_second = current_second
                self.emails_sent_in_last_second = 0
            self.emails_sent_in_last_second += 1

    def wait_for_rate_limit(self):
        with self.rate_limit_lock:
            while self.emails_sent_in_last_second >= self.rate_limit:
                time.sleep(0.1)
                current_second = int(time.time())
                if current_second != self.last_second:
                    self.last_second = current_second
                    self.emails_sent_in_last_second = 0

    def send_email(
        self,
        smtp_server,
        from_email,
        to_email,
        display_name,
        cc,
        reply_to,
        subject,
        body,
        server_url,
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
        msg["Reply-To"] = ", ".join([cc, reply_to])
        mail_time = formatdate(timeval=None, localtime=True)
        msg["Date"] = mail_time

        with self.lock:
            try:
                error = smtp_server.sendmail(from_email, to_email, msg.as_string())
                print("Error: ", error)

                db.register_email(to_email, unique_id)
            except Exception as e:
                logger.error(e)
                logger.error("Failed to send mail: %s", to_email)

    def worker(self):
        server = self.connect_to_smtp_server()

        while True:
            (
                from_email,
                to_email,
                display_name,
                cc,
                reply_to,
                subject,
                body,
                server_url,
            ) = self.queue.get()
            self.wait_for_rate_limit()
            self.send_email(
                server,
                from_email,
                to_email,
                display_name,
                cc,
                reply_to,
                subject,
                body,
                server_url,
            )
            self.increment_emails_sent()
            self.queue.task_done()

    def start(self, num_workers):
        threads = []
        for _ in range(num_workers):
            thread = threading.Thread(target=self.worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)

    def add_email(
        self,
        from_email,
        to_email,
        display_name,
        cc,
        reply_to,
        subject,
        body,
        server_url,
    ):
        email_task = (
            from_email,
            to_email,
            display_name,
            cc,
            reply_to,
            subject,
            body,
            server_url,
        )
        if to_email not in self.emails_sent:
            self.emails_sent.add(to_email)
            self.queue.put(email_task)
        else:
            print("Already sent to: ", to_email)

    def wait(self):
        self.queue.join()

    def close_server_connection(self):
        if self.smtp_server:
            self.smtp_server.quit()
            self.smtp_server = None
        else:
            logger.info("SMTP server connection already closed.")


def read_emails(file_path, offset=0, limit=None):
    """
    Reads emails from a text file and returns a list of emails with offset and limit.

    Args:
        file_path (str): Path to the text file containing emails.
        offset (int, optional): Number of emails to skip. Defaults to 0.
        limit (int, optional): Maximum number of emails to return. Defaults to None.

    Returns:
        list: List of emails.
    """
    try:
        with open(file_path, "r") as file:
            emails = [line.strip() for line in file.readlines()]
            if limit is not None:
                return emails[offset : offset + limit]
            else:
                return emails[offset:]
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []


""" company_details = db.get_company_details(limit=page_size, offset=offset)

        print("Company Details: ", len(company_details))
        if not company_details:
            break

        # Send emails to each company
        for company in company_details:
            emails = company[1].strip()
            if emails == "-NA-" or emails == "":
                continue

            arr = emails.split(";")
            for email in arr:
                email = email.strip().lower()
                if email == "" or email == "-na-":
                    continue
                email_sender.add_email(
                    email_config.email_address,
                    email.strip().lower(),
                    email_config.display_name,
                    email_config.cc,
                    email_config.reply_to,
                    email_config.subject,
                    email_config.body,
                    email_config.server_url,
                )

        print("Emails in queue: ", email_sender.queue.qsize())
        print("Emails in list: ", len(email_sender.emails_sent))
        # break

        email_sender.wait()

        offset += page_size
        page += 1 """


def main(email_config: Config):
    # Example email data
    """
    Get paginated company details from database
    and send emails to each company.
    """

    start_time = time.time()
    logger.info("Start time: %s", start_time)

    email_sender = EmailSender(
        email_config.smtp_server,
        587,  # SMTP port for TLS
        email_config.smtp_username,
        email_config.smtp_password,
    )
    email_sender.start(num_workers=15)

    page = 1
    page_size = 1000
    offset = 6000

    while True:
        if page > 3:
            break

        logger.info("Page: %d", page)
        logger.info("Offset: %d", offset)

        # Get company details from file
        emails_list = read_emails("emails.txt", limit=page_size, offset=offset)

        print("Company Details: ", len(emails_list))
        if not emails_list:
            logger.info("No more emails to process.")
            break

        # Send emails to each company
        for email in emails_list:
            email_sender.add_email(
                email_config.email_address,
                email.strip(),
                email_config.display_name,
                email_config.cc,
                email_config.reply_to,
                email_config.subject,
                email_config.body,
                email_config.server_url,
            )

        print("Emails in queue: ", email_sender.queue.qsize())
        print("Emails in list: ", len(email_sender.emails_sent))
        # break

        email_sender.wait()

        offset += page_size
        page += 1

    db.close()
    email_sender.close_server_connection()
    logger.info("SMTP server connection closed.")

    end_time = time.time()
    logger.info("End time: %s", end_time)

    logger.info("Total time taken: %s", end_time - start_time)
    logger.info("Total emails sent: %d", len(email_sender.emails_sent))

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
            email_params.get("reply-to"),
            aws_smtp_config.get("smtp_username"),
            aws_smtp_config.get("smtp_password"),
            aws_smtp_config.get("smtp_server"),
            email_params.get("cc"),
            cfg.get("server_url"),
        )

    main(email_config)
