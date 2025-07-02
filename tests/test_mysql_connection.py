from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="env.clark")

print("DEBUG:", os.getenv("CLARK_AUTH_PROJECT_ID"))

from clark_secrets import retrieve_project_secrets, ClarkSecretsConfig

config = ClarkSecretsConfig()
secrets = retrieve_project_secrets(config)

import pandas as pd

TABLES = [
    "PracticeFusionPatientDiagnosis",
    "PracticeFusionPatientMedication",
    "assessments",
    "superbill_report",
]

def safe_query(conn, table):
    try:
        query = f"SELECT * FROM `{table}` LIMIT 5"
        df = pd.read_sql(query, conn)
        print(f"\n--- Top 5 rows from `{table}` ---")
        print("Columns:", df.columns.tolist())
        print(df)
    except Exception as e:
        print(f"Error querying table `{table}`: {e}")

def test_mysql_query_tables():
    import sqlalchemy
    engine_str = (
        f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 3306)}/{os.getenv('DB_NAME')}"
    )
    engine = sqlalchemy.create_engine(engine_str)

    for table in TABLES:
        safe_query(engine, table)

    engine.dispose()

if __name__ == "__main__":
    test_mysql_query_tables()
