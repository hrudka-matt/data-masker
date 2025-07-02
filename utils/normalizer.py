import yaml
import os

def load_aliases(yaml_path=None):
    if not yaml_path:
        this_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_path = os.path.normpath(os.path.join(this_dir, '..', 'config', 'column_aliases.yaml'))
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def normalize_columns(df, col_aliases):
    import re
    col_lookup = {}
    for std_col, aliases in col_aliases.items():
        for alias in aliases:
            norm_alias = re.sub(r'[^a-z0-9]', '', alias.lower())
            col_lookup[norm_alias] = std_col

    normalized_cols = {}
    for col in df.columns:
        norm_col = re.sub(r'[^a-z0-9]', '', col.lower())
        std = col_lookup.get(norm_col)
        if std:
            normalized_cols[col] = std

    df = df.rename(columns=normalized_cols)
    wanted = list(col_aliases.keys())
    missing = [col for col in wanted if col not in df.columns]
    if missing:
        print(f"[WARN] Could not find expected columns: {missing}")
    return df
