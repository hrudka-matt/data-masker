# Patient-Mocker

## Overview

Patient-Mocker is a Python project designed to securely fetch patient data from Salesforce and associated MySQL databases, then generate masked (faker) versions of that data to protect sensitive information while preserving relational integrity.

## Features

- Connects to Salesforce using OAuth client credentials.
- Queries patient records from Salesforce.
- Queries related patient data from multiple MySQL tables based on Salesforce patient IDs.
- Applies consistent Faker-based data masking across all datasets.
- Outputs both real and masked data CSVs for auditing and testing.
- Supports configurable environment variables and secret retrieval via Clark Auth.

## File Structure
```
Patient-Mocker/
├── config/
│ ├── column_aliases.yaml
├── connectors/
│ ├── salesforce.py
│ ├── mysql.py
├── utils/
│ ├── init.py
│ ├── faker_map.py
│ ├── io.py
│ ├── normalizer.py
├── env.clark
├── .env
├── main.py
├── mock_salesforcesql.py
├── requirements.txt
├── README.md
```
## Setup Instructions

1. **Clone or download the repository** to your local machine.

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:

    Create `.env.clark` file

    Create a file named `.env.clark` in the root of this project containing the following environment variables:

    ```env
    CLARK_AUTH_PROJECT_ID=your_project_id_here
    CLARK_AUTH_CLIENT_ID=your_client_id_here
    CLARK_AUTH_CLIENT_SECRET=your_client_secret_here
    CLARK_AUTH_USER_NAME=your_user_email_here
    CLARK_AUTH_PASSWORD=your_password_here
    CLARK_AUTH_EMAIL=your_email_here  # optional, defaults to USER_NAME if omitted
    ```

Note: These values can be obtained from your Clark Auth dashboard or administrator.

5. Run the data masking script:

All scripts automatically load .env.clark to authenticate with Clark Auth and fetch required secrets.

Mock patient data and assessments:

```
python mock_patients.py
```
Mock Salesforce and related SQL tables:

```
python mock_salesforcesql.py
```