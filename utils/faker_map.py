from faker import Faker

def make_faker_map(keys, seed=42):
    fake = Faker()
    Faker.seed(seed)
    mapping = {}
    for real_key in keys:
        mapping[real_key] = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d"),
            "gender": fake.random_element(elements=('M', 'F', 'Other')),
            "address": fake.address().replace("\n", ", "),
        }
    return mapping

def apply_faker(df, key_col, fake_map):
    # Replace matching fields with their faked values
    for field in ["first_name", "last_name", "dob", "gender", "address"]:
        if field in df.columns:
            df[field] = df[key_col].map(lambda k: fake_map.get(k, {}).get(field, ""))
    return df
