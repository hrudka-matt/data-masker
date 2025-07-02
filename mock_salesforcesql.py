import os
from dotenv import load_dotenv
import pandas as pd
from clark_secrets import retrieve_project_secrets, ClarkSecretsConfig
from simple_salesforce import Salesforce
import requests
import sqlalchemy
from faker import Faker

OUTPUT_DIR = "mocked_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def save_df_csv(df: pd.DataFrame, filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"Saved {len(df)} rows to {path}")

def make_faker_map(keys):
    fake = Faker()
    Faker.seed(42)
    mapping = {}
    for k in keys:
        mapping[k] = {
            "patient_id": fake.unique.bothify(text='???####'),
            "patient_record_number": fake.unique.bothify(text='###-####'),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d"),
            "patient_name": fake.name(),
            "patient_date_of_birth_date_time": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d"),
            "status": fake.random_element(elements=["Scheduled", "Completed", "Cancelled", "No Show"]),
            "appointment_type_name": fake.random_element(elements=["General Checkup", "Consultation", "Follow-up", "Urgent Care"]),
            "provider_name": fake.name(),
            "facility_name": fake.company() + " Medical Center",
            "patient_home_phone": fake.phone_number(),
            "Practitioner__c": fake.name(),
            "diagnosis": fake.catch_phrase(),  
            "drug_name": fake.word().capitalize(),  
            "generic_name": fake.word().capitalize(),            
        }
    return mapping

def apply_masking(df, fake_map, id_cols, sf_id_col='Patient__c'):
    for idx, row in df.iterrows():
        pid = row.get(sf_id_col)
        if pid in fake_map:
            fake_info = fake_map[pid]
            for col in df.columns:
                if col in id_cols:
                    continue  # keep IDs/GUIDs unchanged
                if col in fake_info:
                    df.at[idx, col] = fake_info[col]
                else:
                    df.at[idx, col] = "MASKED"
    return df

def apply_faker_to_sf(df, fake_map):
    for idx, row in df.iterrows():
        sf_id = row["Id"]
        if sf_id in fake_map:
            fake_info = fake_map[sf_id]
            df.at[idx, "Patient_ID__c"] = fake_info.get("patient_id", "")
            df.at[idx, "Patient_Record_Number__c"] = fake_info.get("patient_record_number", "")
            df.at[idx, "First_Name__c"] = fake_info.get("first_name", "")
            df.at[idx, "Last_Name__c"] = fake_info.get("last_name", "")
            df.at[idx, "DOB__c"] = fake_info.get("dob", "")
    return df

def main():
    # Load env and secrets
    load_dotenv(dotenv_path="env.clark")
    config = ClarkSecretsConfig()
    secrets = retrieve_project_secrets(config)

    # Salesforce OAuth token
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
    sf = Salesforce(instance_url=instance_url, session_id=access_token)

    # Query Salesforce Patients
    soql = """
    SELECT
        Id,
        Practice_GUID__c,
        Patient_ID__c,
        Patient_Record_Number__c,
        First_Name__c,
        Last_Name__c,
        DOB__c,
        Facility__c
    FROM Patient__c
    ORDER BY Id
    LIMIT 10
    """
    sf_result = sf.query(soql)
    sf_patients = pd.DataFrame(sf_result['records']).drop(columns='attributes', errors='ignore')
    save_df_csv(sf_patients, "salesforce_patients_real.csv")

    patient_ids = sf_patients["Id"].tolist()
    if not patient_ids:
        print("No patient IDs from Salesforce; exiting.")
        return
    patient_ids_sql = ",".join(f"'{pid}'" for pid in patient_ids)

    # Create Faker map keyed by Salesforce Id
    fake_map = make_faker_map(patient_ids)

    # Create and save mocked Salesforce patients CSV
    sf_patients_masked = apply_faker_to_sf(sf_patients.copy(), fake_map)
    save_df_csv(sf_patients_masked, "salesforce_patients_mock.csv")

    # Setup MySQL
    engine_str = (
        f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 3306)}/{os.getenv('DB_NAME')}"
    )
    engine = sqlalchemy.create_engine(engine_str)

    # Tables to query
    tables = [
        "PracticeFusionPatientDiagnosis",
        "PracticeFusionPatientMedication",
        "assessments",
        "superbill_report"
    ]

    # Id/GUID columns to preserve (unmasked)
    id_cols_map = {
        "PracticeFusionPatientDiagnosis": {"id", "patient_salesforce_id", "patient_practice_guid"},
        "PracticeFusionPatientMedication": {"id", "patient_salesforce_id", "practice_uuid"},
        "assessments": {"id", "scheduled_event_guid", "patient_practice_guid", "Patient__c", "Facility__c", "provider_guid", "facility_guid"},
        "superbill_report": {"noteId"}
    }

    for table in tables:
        # Adjust patient id column per table
        patient_id_col = "Patient__c"
        if table == "PracticeFusionPatientDiagnosis" or table == "PracticeFusionPatientMedication":
            patient_id_col = "patient_salesforce_id"
        elif table == "superbill_report":
            patient_id_col = "patientIdDisplay"

        query = f"""
        SELECT * FROM `{table}`
        WHERE {patient_id_col} IN ({patient_ids_sql})
        LIMIT 100
        """
        df = pd.read_sql(query, engine)
        if df.empty:
            print(f"No data found for table {table}")
            continue

        save_df_csv(df, f"{table}_real.csv")

        print(f"Masking data for table {table} with {len(df)} rows")
        masked_df = apply_masking(df.copy(), fake_map, id_cols_map.get(table, set()), sf_id_col=patient_id_col)

        save_df_csv(masked_df, f"{table}_mock.csv")

    engine.dispose()
    print("✅ All data masked and saved.")

if __name__ == "__main__":
    main()
