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
        )
        self.cursor = self.conn.cursor()

    def create_tables(self):
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tracking_data
            (id SERIAL PRIMARY KEY, unique_id TEXT UNIQUE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            
            CREATE TABLE IF NOT EXISTS email_data
            (id SERIAL PRIMARY KEY, email_address TEXT, unique_id TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
            """
        )
        self.conn.commit()

    def track_email(self, unique_id):
        self.cursor.execute(
            "SELECT * FROM tracking_data WHERE unique_id=%s", (unique_id,)
        )
        if self.cursor.fetchone():
            return "Email already tracked"
        else:
            self.cursor.execute(
                "INSERT INTO tracking_data (unique_id) VALUES (%s)", (unique_id,)
            )
            self.conn.commit()
            return "Email tracked successfully"
        
    def register_email(self, email_address, unique_id):
        self.cursor.execute(
            "INSERT INTO email_data (email_address, unique_id) VALUES (%s, %s)",
            (email_address, unique_id),
        )
        self.conn.commit()
        return "Email registered successfully"