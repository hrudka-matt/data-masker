from dotenv import load_dotenv
import os

# 1. Load environment variables from env.clark
load_dotenv(dotenv_path="env.clark")

print("DEBUG:", os.getenv("CLARK_AUTH_PROJECT_ID"))

from clark_secrets import retrieve_project_secrets, ClarkSecretsConfig

# 2. Retrieve Salesforce secrets using ClarkSecrets
config = ClarkSecretsConfig()  
secrets = retrieve_project_secrets(config)

from simple_salesforce import Salesforce
import requests

# 3. Get OAuth access token from Salesforce
token_url = secrets["SF_URL"] + "/services/oauth2/token"
payload = {
    'grant_type': 'client_credentials',
    'client_id': secrets["SF_CLIENT_ID"],
    'client_secret': secrets["SF_CLIENT_SECRET"]
}
response = requests.post(token_url, data=payload)
response.raise_for_status()
token_data = response.json()
access_token = token_data["access_token"]
instance_url = token_data.get("instance_url", secrets["SF_URL"])

# 4. Instantiate Salesforce client
sf = Salesforce(instance_url=instance_url, session_id=access_token)

# 5. Build SOQL query for Patient__c and related Facility__c fields
from dotenv import load_dotenv
import os

# 1. Load environment variables from env.clark
load_dotenv(dotenv_path="env.clark")

from clark_secrets import retrieve_project_secrets, ClarkSecretsConfig

# 2. Retrieve Salesforce secrets using ClarkSecrets
config = ClarkSecretsConfig() 
secrets = retrieve_project_secrets(config)

from simple_salesforce import Salesforce
import requests

# 3. Get OAuth access token from Salesforce
token_url = secrets["SF_URL"] + "/services/oauth2/token"
payload = {
    'grant_type': 'client_credentials',
    'client_id': secrets["SF_CLIENT_ID"],
    'client_secret': secrets["SF_CLIENT_SECRET"]
}
response = requests.post(token_url, data=payload)
response.raise_for_status()
token_data = response.json()
access_token = token_data["access_token"]
instance_url = token_data.get("instance_url", secrets["SF_URL"])

# 4. Instantiate Salesforce client
sf = Salesforce(instance_url=instance_url, session_id=access_token)

# 5. SOQL Query
soql = """
SELECT
    Id,
    First_Name__c,
    Last_Name__c,
    DOB__c,
    Gender__c,
    Practice_GUID__c,
    Patient_ID__c,
    Patient_Record_Number__c,
    Facility__c,
    Facility__r.Id,
    Facility__r.Name,
    Facility__r.Practice_GUID__c,
    Address__Street__s,
    Address__City__s,
    Address__PostalCode__s,
    Address__StateCode__s,
    Address__CountryCode__s,
    Medicaid_ID__c,
    Medicaid_Payer__c,
    Medicaid_Payer_ID__c
FROM Patient__c
LIMIT 5
"""


result = sf.query(soql)
print(f"Returned {result['totalSize']} patients")
for row in result['records']:
    print(row)
