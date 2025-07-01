import re
import yaml
import os

def load_aliases(yaml_path=None):
    # Always resolve path relative to project root, not cwd
    if not yaml_path:
        # Use absolute path from script's own location
        this_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_path = os.path.normpath(os.path.join(this_dir, '..', 'config', 'column_aliases.yaml'))
    with open(yaml_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def normalize_columns(df, col_aliases):
    # Map all aliases (normalized) to their canonical name
    col_lookup = {}
    for std_col, aliases in col_aliases.items():
        for alias in aliases:
            norm_alias = re.sub(r'[^a-z0-9]', '', alias.lower())
            col_lookup[norm_alias] = std_col
    # Build rename dict
    normalized_cols = {}
    for col in df.columns:
        norm_col = re.sub(r'[^a-z0-9]', '', col.lower())
        std = col_lookup.get(norm_col)
        if std:
            normalized_cols[col] = std  # Rename THIS column to standard
    df = df.rename(columns=normalized_cols)
    # Only keep columns that were normalized
    wanted = [col for col in col_aliases]
    missing = [col for col in wanted if col not in df.columns]
    if missing:
        print(f"[WARN] Could not find: {missing} in columns {list(df.columns)}")
    return df[[col for col in wanted if col in df.columns]]