import time
import pandas as pd
from database import Database

db = Database("config.json")


def get_company_details(sheet_numer):
    print("Reading company details from excel file...")
    print("Sheet Number: ", sheet_numer)

    file = pd.ExcelFile(r"D:\aargo\Nbfc .xlsx")
    df = file.parse(f"Sheet {sheet_numer}")

    file.close()

    df.columns = [
        "ID",
        "Name",
        "Location",
        "D",
        "E",
        "Registration Number",
        "G",
        "Address",
        "Email",
    ]

    company_details = []

    for index, row in df.iterrows():
        company = {
            "name": row["Name"],
            "location": row["Location"],
            "registration_number": row["Registration Number"],
            "address": row["Address"],
            "email": row["Email"],
        }

        company_details.append(company)

    return company_details


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


def get_read_emails(file_path):
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
            return emails
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []


""" def main():
    for i in range(31, 32):
        company_details = get_company_details(i)
        
        db = Database("config.json")
        
        db.bulk_insert_company_details(company_details)
        print("Company Details Stored")
        
        db.close()
        
        time.sleep(1) """


def get_emails_from_db(offset=0, limit=None, exclusion_set=None):
    """
    Fetches emails from the database and writes them to a text file.

    Args:
        offset (int, optional): Number of emails to skip. Defaults to 0.
        limit (int, optional): Maximum number of emails to return. Defaults to None.

    Returns:
        None
    """
    page_size = limit if limit is not None else 1000
    offset = 0

    db = Database("config_new.json")
    emails_list = set()

    while True:
        company_details = db.get_company_details(limit=page_size, offset=offset)
        if len(company_details) == 0:
            break

        print("Processing: ", offset, " to ", offset + page_size)

        for company in company_details:
            emails = company[1]

            if emails == "-NA-" or emails == "":
                print("No Emails Found: ", company[0])
                continue

            arr = emails.split(";")
            for email in arr:
                email = email.strip().lower()
                if email == "" or email == "-na-":
                    continue

                if email in exclusion_set:
                    print("Email already read: ", email)
                    continue

                if email not in emails_list:
                    emails_list.add(email)
                    with open("emails.txt", "a") as f:
                        f.write(email + "\n")

        print(f"{page_size} Entries processed")
        print("Emails in list: ", len(emails_list))

        offset += page_size
        time.sleep(1)


def main():
    exclusion_set = get_read_emails("email_reads.txt")
    get_emails_from_db(offset=0, limit=1000, exclusion_set=exclusion_set)

    file_path = "emails.txt"
    offset = 0
    limit = 1000

    emails = read_emails(file_path, offset, limit)
    print("Emails: ", len(emails))
    print(emails[0])
    print(emails[len(emails)-1])


if __name__ == "__main__":
    main()
