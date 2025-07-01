import pandas as pd
import mysql.connector

def fetch_mysql_patients(config):
    conn = mysql.connector.connect(
        host=config["host"],
        port=config.get("port", 3306),
        user=config["user"],
        password=config["password"],
        database=config["database"],
    )
    query = f"SELECT * FROM {config['table']}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df
