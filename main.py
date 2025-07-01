import yaml
import pandas as pd
from connectors.salesforce import fetch_salesforce_patients
from connectors.mysql_db import fetch_mysql_patients
from utils.faker_map import make_faker_map, apply_faker
from utils.io import export_csv

def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

def main():
    cfg = load_config()
    print("[INFO] Fetching from Salesforce...")
    df_sf = fetch_salesforce_patients(cfg["salesforce"])
    print(f"[INFO] Salesforce records: {len(df_sf)}")

    print("[INFO] Fetching from MySQL...")
    df_sql = fetch_mysql_patients(cfg["mysql"])
    print(f"[INFO] MySQL records: {len(df_sql)}")

    # Combine patient keys from both sources (MRNs)
    mock_key = cfg.get("mock_key", "MRN")
    patient_keys = pd.concat([df_sf[mock_key], df_sql[mock_key]]).dropna().unique()
    print(f"[INFO] Found {len(patient_keys)} unique {mock_key}s.")

    # Build fake mapping
    fake_map = make_faker_map(patient_keys)

    # Apply to both DataFrames
    df_sf_fake = apply_faker(df_sf.copy(), mock_key, fake_map)
    df_sql_fake = apply_faker(df_sql.copy(), mock_key, fake_map)

    # (Optional) Combine, or save each as needed
    df_all = pd.concat([df_sf_fake, df_sql_fake], ignore_index=True)
    export_csv(df_all, cfg["output_csv"])

if __name__ == "__main__":
    main()
