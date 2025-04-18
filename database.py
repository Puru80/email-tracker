import psycopg2
import json


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
                    port=config["database"]["port"]
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
            self.cursor.execute("SELECT * FROM email_data WHERE unique_id=%s", (unique_id,))
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
            print("Unique ID: ", unique_id)
            return "successfully"
        except psycopg2.InterfaceError as e:
            print(e)
            print("Email: ", email_address)
            print("Unique ID: ", unique_id)
        except Exception as e:
            print(e)
            print("Email: ", email_address)
            print("Unique ID: ", unique_id)

    def close(self):
        self.cursor.close()
        self.conn.close()
