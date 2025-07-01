import pandas as pd
from simple_salesforce import Salesforce

def fetch_salesforce_patients(config):
    sf = Salesforce(
        username=config["username"],
        password=config["password"],
        security_token=config["security_token"],
        domain=config.get("domain", "login")
    )
    result = sf.query(config["soql"])
    records = result['records']
    df = pd.DataFrame(records)
    # Remove Salesforce metadata fields
    df = df.drop(columns=["attributes"], errors="ignore")
    return df
