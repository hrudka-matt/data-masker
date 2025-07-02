import os
import pandas as pd
import mysql.connector

def fetch_mysql_patients(config):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )
    query = f"SELECT * FROM {config['table']}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df
