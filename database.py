import psycopg2
import json


class Database:

    def __init__(self, config_file):
        with open(config_file, "r") as f:
            config = json.load(f)

        self.conn = psycopg2.connect(
            dbname=config["database"]["dbname"],
            user=config["database"]["user"],
            password=config["database"]["password"],
            host=config["database"]["host"],
            port=config["database"]["port"],
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        self.cursor = self.conn.cursor()

    def health_check(self):
        self.cursor.execute("SELECT 1")
        return "Database connection successful"

    def create_tables(self):
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

    def track_email(self, unique_id):
        try: 
            self.cursor.execute("SELECT * FROM email_data WHERE unique_id=%s", (unique_id,))
            record = self.cursor.fetchone()
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
            return "Email registered successfully"
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
