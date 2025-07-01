import pandas as pd

def export_csv(df, path):
    df.to_csv(path, index=False)
    print(f"[INFO] Exported to {path}")
