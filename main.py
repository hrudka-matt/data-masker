from dotenv import load_dotenv
import os
from clark_secrets import retrieve_project_secrets, ClarkSecretsConfig, write_env_file

def bootstrap_secrets():
    load_dotenv(dotenv_path="env.clark")  

    config = ClarkSecretsConfig(
        project_id=os.getenv("CLARK_AUTH_PROJECT_ID"),
        client_id=os.getenv("CLARK_AUTH_CLIENT_ID"),
        client_secret=os.getenv("CLARK_AUTH_CLIENT_SECRET"),
        user_name=os.getenv("CLARK_AUTH_USER_NAME"),
        password=os.getenv("CLARK_AUTH_PASSWORD"),
        email=os.getenv("CLARK_AUTH_EMAIL")
    )

    secrets = retrieve_project_secrets(config)
    return secrets

if __name__ == "__main__":
    secrets = bootstrap_secrets()
    print("✅ Clark secrets retrieved successfully.")
