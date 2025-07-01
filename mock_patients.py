import pandas as pd
import argparse
from utils.normalizer import load_aliases, normalize_columns
from utils.faker_map import make_faker_map, apply_faker

def main(salesforce_csv, mysql_csv, output_csv, aliases_path):
    # 1. Load column aliases
    aliases = load_aliases(aliases_path)

    # 2. Load and normalize Salesforce data
    salesforce_df = pd.read_csv(salesforce_csv)
    salesforce_df = normalize_columns(salesforce_df, aliases)

    # 3. Load and normalize MySQL data
    mysql_df = pd.read_csv(mysql_csv)
    mysql_df = normalize_columns(mysql_df, aliases)

    # 4. Combine all patients and dedupe
    key_col = "mrn"
    all_patients = pd.concat([salesforce_df, mysql_df], ignore_index=True)
    all_patients = all_patients.drop_duplicates(key_col)

    # 5. Generate and apply fake map
    fake_map = make_faker_map(all_patients[key_col])
    masked_patients = apply_faker(all_patients.copy(), key_col, fake_map)

    # 6. Write out masked file
    masked_patients.to_csv(output_csv, index=False)
    print(f"✅ Masked patient data written to {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock and mask patient data from Salesforce and MySQL sources.")
    parser.add_argument("--salesforce", default="tests/test_data/salesforce_sample.csv", help="Salesforce CSV path")
    parser.add_argument("--mysql", default="tests/test_data/mysql_sample.csv", help="MySQL CSV path")
    parser.add_argument("--output", default="tests/test_data/mocked_output.csv", help="Output CSV path")
    parser.add_argument("--aliases", default="config/column_aliases.yaml", help="YAML file for column aliases")
    args = parser.parse_args()

    main(args.salesforce, args.mysql, args.output, args.aliases)
