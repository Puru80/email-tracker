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
    
    """ for index, row in df.iterrows():
        emails = []
        email_str = row["Email"]
        
        if email_str == "-NA-" or email_str == "" or email_str == "nan":
            print("Skipping nan: ", email_str)
            continue
        arr = email_str.split(";")

        for email in arr:
            if len(email.strip()) > 0:
                emails.append(email.strip().lower())

        for email in emails: 
            
            company = {
                "name": row["Name"],
                "location": row["Location"],
                "registration_number": row["Registration Number"],
                "address": row["Address"],
                "email": email,
            }

            company_details.append(company) """
            
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


def main():
    for i in range(31, 32):
        company_details = get_company_details(i)
        # print("Company Details: ", company_details)

        # for company in company_details:
        #     db.store_company_details(company)
            # print("Company Details Stored: ", company)
            
        db = Database("config.json")
        
        db.bulk_insert_company_details(company_details)
        print("Company Details Stored")
        
        db.close()
        
        time.sleep(1)

if __name__ == "__main__":
    main()
