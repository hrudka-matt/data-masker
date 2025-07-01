import sys
import os
import pandas as pd

# Make sure we can import from ../utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.faker_map import make_faker_map, apply_faker

def test_mocking_pipeline():
    # Load test data
    salesforce_df = pd.read_csv("tests/test_data/salesforce_sample.csv")
    mysql_df = pd.read_csv("tests/test_data/mysql_sample.csv")

    # Normalize columns
    salesforce_df.columns = ['mrn', 'first_name', 'last_name', 'dob', 'gender', 'address']
    mysql_df.columns = ['mrn', 'first_name', 'last_name', 'dob', 'gender', 'address']

    # Combine (simulate full patient set)
    all_patients = pd.concat([salesforce_df, mysql_df]).drop_duplicates("mrn")

    # Make the fake mapping
    fake_map = make_faker_map(all_patients['mrn'].unique())

    # Apply fakes
    mocked_patients = apply_faker(all_patients.copy(), 'mrn', fake_map)

    # Test: all real patient names should be replaced (allow a few accidental matches)
    real_names = set(salesforce_df['first_name']).union(mysql_df['first_name'])
    overlap = set(mocked_patients['first_name']) & real_names
    if overlap:
        print(f"⚠️ WARNING: Some fake first names matched real names: {overlap}")
    assert len(overlap) < 3, f"Too many overlaps in first names: {overlap}"

    # Test: MRN should remain consistent
    assert set(mocked_patients['mrn']) == set(all_patients['mrn'])

    # Test: No nulls in critical columns
    for col in ["first_name", "last_name", "dob"]:
        assert mocked_patients[col].notnull().all()

    # Optionally, write the result and manually inspect:
    out_path = "tests/test_data/mocked_output.csv"
    mocked_patients.to_csv(out_path, index=False)

    print(f"✅ Test passed: Mocked data written to {out_path}")

if __name__ == "__main__":
    test_mocking_pipeline()
