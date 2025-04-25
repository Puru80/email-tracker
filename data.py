import time
import pandas as pd
from database import Database


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


""" def main():
    for i in range(31, 32):
        company_details = get_company_details(i)
        
        db = Database("config.json")
        
        db.bulk_insert_company_details(company_details)
        print("Company Details Stored")
        
        db.close()
        
        time.sleep(1) """


def main():
    page_size = 1000
    offset = 0

    db = Database("config_new.json")

    while True:
        company_details = db.get_company_details(limit=page_size, offset=offset)
        if len(company_details) == 0:
            break

        for company in company_details:
            emails = company[1]
            if emails == "-NA-" or emails == "":
                continue
            arr = emails.split(";")
            for email in arr:
                if email == "":
                    continue

                with open("emails.txt", "a") as f:
                    f.write(email.strip().lower() + "\n")

        print("1000 Entries processed")

        offset += page_size
        time.sleep(1)


if __name__ == "__main__":
    main()
