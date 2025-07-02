from faker import Faker

def make_faker_map(keys, seed=42):
    from faker import Faker
    fake = Faker()
    Faker.seed(seed)
    mapping = {}
    for real_key in keys:
        patient_id = fake.unique.bothify(text="???###???##").upper()
        practice_guid = fake.uuid4()
        mapping[real_key] = {
            "fake_id": patient_id,  # use this as fake Salesforce ID and Patient__c
            "practice_guid": practice_guid,
            "patient_id": patient_id,
            "patient_record_number": fake.unique.bothify(text="###-####"),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d"),
            "provider_name": "Dr. " + fake.last_name(),
            "facility_guid": "FAC-" + fake.bothify(text="???").upper(),
            "facility_name": fake.company() + " Medical Center",
            "practitioner": "Practitioner " + fake.first_name(),
            "appointment_type": fake.random_element(elements=["General Checkup", "Follow-up", "Consultation", "Emergency"]),
            "gender": fake.random_element(elements=('M', 'F', 'Other')),
            "address": fake.address().replace("\n", ", "),
        }
    return mapping



def apply_faker_masking(df, fake_map):
    for idx, row in df.iterrows():
        mrn = row["Patient_ID__c"]
        if mrn in fake_map:
            fake_info = fake_map[mrn]
            # Fake IDs - unique but consistent
            fake_id = fake_info.get("fake_id", None)
            if not fake_id:
                # fallback if not in faker map (optional)
                fake_id = fake_info["patient_id"].replace("-", "")[:15]  # or generate new

            # Assign same fake_id to Id and Patient__c
            df.at[idx, "Id"] = fake_id
            df.at[idx, "Patient__c"] = fake_id

            # Fake Practice_GUID (can be UUID-like string)
            df.at[idx, "Practice_GUID__c"] = fake_info.get("practice_guid", fake_info["patient_id"])

            # Fake other linked fields
            df.at[idx, "Patient_ID__c"] = fake_info["patient_id"]
            df.at[idx, "Patient_Record_Number__c"] = fake_info["patient_record_number"]
            df.at[idx, "First_Name__c"] = fake_info["first_name"]
            df.at[idx, "Last_Name__c"] = fake_info["last_name"]
            df.at[idx, "DOB__c"] = fake_info["dob"]
            df.at[idx, "patient_name"] = f"{fake_info['first_name']} {fake_info['last_name']}"

            df.at[idx, "provider_name"] = fake_info["provider_name"]
            df.at[idx, "facility_guid"] = fake_info["facility_guid"]
            df.at[idx, "facility_name"] = fake_info["facility_name"]
            df.at[idx, "practitioner__c"] = fake_info["practitioner"]
            df.at[idx, "appointment_type_name"] = fake_info["appointment_type"]
    return df