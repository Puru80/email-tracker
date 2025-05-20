import psycopg2
from psycopg2 import sql
import json
import logging


# Logger setup
logger = logging.getLogger("email_sender")
logger.setLevel(logging.INFO)

# Create a file handler
file_handler = logging.FileHandler("email_status.log")
file_handler.setLevel(logging.INFO)

# Create a formatter and set it for the file handler
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)


class Database:

    def __init__(self, config_file):
        with open(config_file, "r") as f:
            config = json.load(f)

        for attempt in range(5):
            print("Connection Attempt: ", attempt + 1)
            try:
                self.conn = psycopg2.connect(
                    dbname=config["database"]["dbname"],
                    user=config["database"]["user"],
                    password=config["database"]["password"],
                    host=config["database"]["host"],
                    port=config["database"]["port"],
                )
                self.cursor = self.conn.cursor()
                break
            except psycopg2.InterfaceError as e:
                print("InterfaceError: ", e)
            except psycopg2.OperationalError as e:
                print("OperationalError", e)
            except Exception as e:
                print("Exception", e)

    def health_check(self):
        try:
            self.cursor.execute("SELECT 1")
            return "Database connection successful"
        except psycopg2.InterfaceError as e:
            print(e)
        except Exception as e:
            print(e)

            return "Database connection failed"

    def create_tables(self):
        try:
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS email_data
                (
                    id            SERIAL PRIMARY KEY,
                    email_address TEXT,
                    unique_id     TEXT,
                    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            self.conn.commit()
        except psycopg2.InterfaceError as e:
            print("InterfaceError", e)
        except Exception as e:
            print(e)

    def track_email(self, unique_id):
        try:
            self.cursor.execute(
                "SELECT * FROM email_data WHERE unique_id=%s", (unique_id,)
            )
            record = self.cursor.fetchone()

            print("Record: ", record)

            if record is None:
                return "Email does not exist"

            if record[4] is not None:
                return "Email already tracked"

            else:
                self.cursor.execute(
                    "UPDATE email_data SET read_at = now() WHERE unique_id = %s",
                    (unique_id,),
                )
                self.conn.commit()
                return "Email tracked successfully"
        except Exception as e:
            print(e)
            print("Unique ID: ", unique_id)
            return str(e)

    def register_email(self, email_address, unique_id):
        try:
            self.cursor.execute(
                "INSERT INTO email_data (email_address, unique_id) VALUES (%s, %s)",
                (email_address, unique_id),
            )
            self.conn.commit()
            print("Email registered successfully")
            return "successfully"
        except psycopg2.InterfaceError as e:
            print(e)
            print("Email: ", email_address)
            print("Unique ID: ", unique_id)
        except Exception as e:
            print(e)
            print("Email: ", email_address)
            print("Unique ID: ", unique_id)

    # Write a function to bulk email register emails
    def bulk_register_emails(self, emails):
        try:
            insert_query = """
                INSERT INTO email_data (email_address, unique_id, created_at)
                VALUES (%s, %s, %s)
            """
            data_to_insert = [
                (email["to_email"], email["unique_id"], email["sent_at"])
                for email in emails
            ]
            self.cursor.executemany(insert_query, data_to_insert)
            self.conn.commit()
            print("Bulk insert of emails successful")
        except Exception as e:
            print(e)

    def get_company_details(self, limit=500, offset=0):
        try:
            self.cursor.execute(
                f"""
                SELECT * FROM company_info
                where status = ('GO')
                order by id
                limit {limit}
                offset {offset}
                """
            )
            records = self.cursor.fetchall()
            return records
        except Exception as e:
            print(e)

    def store_company_details(self, company):
        try:
            self.cursor.execute(
                """
                INSERT INTO company_info (company_name, location, reg_number, address, emails)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    company["name"],
                    company["location"],
                    company["registration_number"],
                    company["address"],
                    company["email"],
                ),
            )
            self.conn.commit()
            print("Company details stored successfully")
        except Exception as e:
            print(e)

    def bulk_insert_company_details(self, company_details):
        try:
            insert_query = """
                INSERT INTO company_info (company_name, location, reg_number, address, emails)
                VALUES (%s, %s, %s, %s, %s)
            """
            data_to_insert = [
                (
                    company["name"],
                    company["location"],
                    company["registration_number"],
                    company["address"],
                    company["email"],
                )
                for company in company_details
            ]
            self.cursor.executemany(insert_query, data_to_insert)
            self.conn.commit()
            print("Bulk insert of company details successful")
        except Exception as e:
            print(e)

    def update_company_status(self, email_list):
        """
        Updates company status to 'BLACKLISTED' for each email in the list.

        Args:
            email_list: List of email strings to update
            db_connection_params: Dictionary containing database connection parameters
                (host, database, user, password, port)
        """
        try:

            for email in email_list:
                try:
                    # Create and execute the update query
                    query = sql.SQL(
                        """
                        UPDATE public.company_info 
                        SET status = ('BLACKLISTED' )
                        WHERE emails ILIKE %s
                    """
                    )
                    self.cursor.execute(query, [f"%{email}%"])
                    self.conn.commit()

                except Exception as e:
                    self.conn.rollback()
                    logging.error(
                        f"Failed to update for email {email}. Error: {str(e)}"
                    )

        except Exception as e:
            logging.critical(f"Database connection failed. Error: {str(e)}")

    def close(self):
        self.cursor.close()
        self.conn.close()
