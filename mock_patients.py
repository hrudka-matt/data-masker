from dotenv import load_dotenv
import os
import pandas as pd
from clark_secrets import retrieve_project_secrets, ClarkSecretsConfig
from simple_salesforce import Salesforce
import requests
import sqlalchemy
from utils.normalizer import load_aliases, normalize_columns
from utils.faker_map import make_faker_map

def main():
    # Step 1: Load env variables and secrets
    load_dotenv(dotenv_path="env.clark")
    config = ClarkSecretsConfig()
    secrets = retrieve_project_secrets(config)

    # Step 2: Connect to Salesforce with OAuth client credentials
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

    # Step 3: Query top 10 patients from Salesforce with needed fields
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

    print("Salesforce raw columns:", sf_patients.columns.tolist())
    print("\n=== Salesforce Patient Data (Top 10 rows) ===")
    print(sf_patients.head(10))

    # Extract Salesforce IDs and Facility IDs for filtering MySQL
    sf_ids = sf_patients["Id"].tolist()
    facility_ids = sf_patients["Facility__c"].dropna().unique().tolist()

    # Step 4: Setup MySQL connection
    engine_str = (
        f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 3306)}/{os.getenv('DB_NAME')}"
    )
    engine = sqlalchemy.create_engine(engine_str)

    # Safety: make sure lists aren't empty before creating SQL filters
    sf_ids_sql = ",".join(f"'{i}'" for i in sf_ids) if sf_ids else "''"
    facility_ids_sql = ",".join(f"'{f}'" for f in facility_ids) if facility_ids else "''"

    # Step 5: Query matching assessments filtering on Patient__c and Facility__c
    mysql_query = f"""
    SELECT
        patient_name,
        patient_date_of_birth_date_time,
        provider_name,
        facility_guid,
        facility_name,
        practitioner__c,
        appointment_type_name,
        Patient__c,
        Facility__c
    FROM assessments
    WHERE Patient__c IN ({sf_ids_sql})
      AND Facility__c IN ({facility_ids_sql})
    ORDER BY Patient__c
    LIMIT 20
    """
    mysql_patients = pd.read_sql(mysql_query, engine)

    print("MySQL raw columns:", mysql_patients.columns.tolist())
    print("\n=== MySQL Assessment Data (Top 10 rows) ===")
    print(mysql_patients.head(10))

    # Step 6: Merge datasets on Patient__c = Id AND Facility__c matching
    merged = pd.merge(
        sf_patients,
        mysql_patients,
        left_on=["Id", "Facility__c"],
        right_on=["Patient__c", "Facility__c"],
        how="inner",
        suffixes=('_sf', '_mysql')
    )

    print(f"Merged data shape: {merged.shape}")
    print("\n=== Merged Data (Top 10 rows) ===")
    print(merged.head(10))

    # Step 7: Create Faker mapping keyed on Patient_ID__c (MRN)
    mrn_keys = merged["Patient_ID__c"].unique()
    fake_map = make_faker_map(mrn_keys)

    # Step 8: Apply Faker masking to all requested columns consistently (except Id and Practice_GUID__c)
    def apply_faker_masking(df, fake_map):
        for idx, row in df.iterrows():
            mrn = row["Patient_ID__c"]
            if mrn in fake_map:
                fake_info = fake_map[mrn]
                # IDs NOT modified:
                # df.at[idx, "Id"] = row["Id"]  # Leave as is
                # df.at[idx, "Practice_GUID__c"] = row["Practice_GUID__c"]  # Leave as is

                # Fake IDs - only Patient_ID__c and Patient_Record_Number__c masked
                df.at[idx, "Patient_ID__c"] = fake_info.get("patient_id", fake_info["first_name"] + "_id")
                df.at[idx, "Patient_Record_Number__c"] = fake_info.get("patient_record_number", fake_info["last_name"] + "_recnum")

                # Fake names & DOB
                df.at[idx, "First_Name__c"] = fake_info["first_name"]
                df.at[idx, "Last_Name__c"] = fake_info["last_name"]
                df.at[idx, "DOB__c"] = fake_info["dob"]

                # Assessment patient name
                df.at[idx, "patient_name"] = f"{fake_info['first_name']} {fake_info['last_name']}"

                # Fake or mask additional fields if needed
                df.at[idx, "provider_name"] = fake_info.get("provider_name", "Dr. " + fake_info["last_name"])
                df.at[idx, "facility_guid"] = fake_info.get("facility_guid", "FAC-" + fake_info["last_name"][:3].upper())
                df.at[idx, "facility_name"] = fake_info.get("facility_name", fake_info["last_name"] + " Medical Center")
                df.at[idx, "practitioner__c"] = fake_info.get("practitioner", "Practitioner " + fake_info["first_name"])
                df.at[idx, "appointment_type_name"] = fake_info.get("appointment_type", "General Checkup")
        return df

    merged_masked = apply_faker_masking(merged.copy(), fake_map)

    # Step 9: Select and rename columns for final output, using masked data but original Id and GUID
    final_df = pd.DataFrame()
    final_df["Id"] = merged["Id"]  # original Salesforce Id
    final_df["MRN"] = merged_masked["Patient_ID__c"]
    final_df["Practice GUID"] = merged["Practice_GUID__c"]  # original GUID
    final_df["Patient Record Number"] = merged_masked["Patient_Record_Number__c"]
    final_df["First Name"] = merged_masked["First_Name__c"]
    final_df["Last Name"] = merged_masked["Last_Name__c"]
    final_df["DOB"] = merged_masked["DOB__c"]
    final_df["Facility ID"] = merged["Facility__c"]  # original Facility Id
    final_df["Assessment Patient Name"] = merged_masked["patient_name"]
    final_df["Assessment DOB"] = merged["patient_date_of_birth_date_time"]  # original assessment DOB
    final_df["Provider Name"] = merged_masked["provider_name"]
    final_df["Facility GUID"] = merged_masked["facility_guid"]
    final_df["Assessment Facility Name"] = merged_masked["facility_name"]
    final_df["Practitioner"] = merged_masked["practitioner__c"]
    final_df["Appointment Type"] = merged_masked["appointment_type_name"]

    # Step 10: Save output masked CSV
    final_df.to_csv("joined_patient_assessment_masked_output.csv", index=False)

    # Step 11: Save both real merged data and masked data as CSV for debug/review,
    # with only the final columns and no raw IDs
    real_output = pd.DataFrame()
    real_output["Id"] = merged["Id"]
    real_output["MRN"] = merged["Patient_ID__c"]
    real_output["Practice GUID"] = merged["Practice_GUID__c"]
    real_output["Patient Record Number"] = merged["Patient_Record_Number__c"]
    real_output["First Name"] = merged["First_Name__c"]
    real_output["Last Name"] = merged["Last_Name__c"]
    real_output["DOB"] = merged["DOB__c"]
    real_output["Facility ID"] = merged["Facility__c"]
    real_output["Assessment Patient Name"] = merged["patient_name"]
    real_output["Assessment DOB"] = merged["patient_date_of_birth_date_time"]
    real_output["Provider Name"] = merged["provider_name"]
    real_output["Facility GUID"] = merged["facility_guid"]
    real_output["Assessment Facility Name"] = merged["facility_name"]
    real_output["Practitioner"] = merged["practitioner__c"]
    real_output["Appointment Type"] = merged["appointment_type_name"]

    real_output.to_csv("merged_real_data.csv", index=False)

    print("✅ Masked patient data saved to 'joined_patient_assessment_masked_output.csv'")
    print("✅ Real merged data saved to 'merged_real_data.csv'")
    print(final_df.head())

if __name__ == "__main__":
    main()
