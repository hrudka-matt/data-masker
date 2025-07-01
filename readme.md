# Patient Data Mocking Utility

This utility connects to Salesforce and MySQL, fetches patient records, creates consistent fake data (mocking PHI/PII fields), and exports a combined CSV file for safe use in development or analytics.

## Features

- Connects to Salesforce and/or MySQL (configurable)
- Identifies unique patients and generates fake PHI/PII (names, MRNs, etc.)
- Replaces original values with consistent fake data
- Outputs a single CSV with combined, mocked records
- Modular, easily extended for more data sources or custom fields

## Getting Started

### 1. Clone and Set Up

```bash
git clone https://github.com/your-org/patient-mocker.git
cd patient-mocker
python -m venv venv
venv\Scripts\activate   
pip install -r requirements.txt
```

2. Configure Connections

Edit config.yaml and provide:

    Salesforce API credentials (client ID/secret, username, password/token)

    MySQL connection string

You can use dummy/test values or skip any source you don't need for now.
3. Run
```bash
python main.py

#mock patients dry run with local data
python mock_patients.py

##Test Plan
python -m tests.test_mocking
```
Output will be saved as mocked_patients.csv by default.

4. Directory Structure
```
patient-mocker/
│
├── config/
│   ├── column_aliases.yaml      # <-- YAML with column alias mappings
│   └── config.yaml             # (optional: other configuration)
│
├── connectors/
│   ├── mysql.py
│   └── salesforce.py
│
├── tests/
│   ├── test_data/
│   │   ├── mysql_example.csv
│   │   └── salesforce_example.csv
│   └── test_mocking.py
│
├── utils/
│   ├── faker_map.py
│   ├── io.py
│   └── normalizer.py
│
├── main.py                  # (optional: main entry point for config-driven run)
├── mock_patients.py         # CLI entry: run mocker from CSVs
├── requirements.txt
└── README.md

```

5. Customization

    Adjust fields in column_aliases.yaml to all the potential variations that could be found.

    Extend connectors or add new ones under connectors/

    Tweak the output format as needed in utils/io.py

    adjust the args parameters to point to a different file location
