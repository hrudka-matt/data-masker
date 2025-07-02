import os
import pandas as pd
import requests
from simple_salesforce import Salesforce

def get_salesforce_access_token():
    """
    Obtain an OAuth2 access token from Salesforce using client credentials.
    Returns (access_token, instance_url)
    """
    token_url = os.environ["SF_URL"] + "/services/oauth2/token"
    payload = {
        'grant_type': 'client_credentials',
        'client_id': os.environ["SF_CLIENT_ID"],
        'client_secret': os.environ["SF_CLIENT_SECRET"]
    }
    response = requests.post(token_url, data=payload)
    response.raise_for_status()
    token_data = response.json()
    instance_url = token_data.get("instance_url", os.environ["SF_URL"])
    return token_data["access_token"], instance_url

def fetch_salesforce_patients(config):
    """
    Fetch patient records from Salesforce using OAuth2.
    `config` is a dict with keys:
      - soql: the SOQL query string
      - object: the Salesforce object name (unused here, but may be useful elsewhere)
    """
    access_token, instance_url = get_salesforce_access_token()
    sf = Salesforce(
        instance_url=instance_url,
        session_id=access_token
    )
    result = sf.query(config["soql"])
    records = result["records"]
    df = pd.DataFrame(records)
    df = df.drop(columns=["attributes"], errors="ignore")
    return df
