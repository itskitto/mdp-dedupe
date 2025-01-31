# MDP Dedupe

A mock process for identifying and merging duplicate patient records across multiple medical data providers.

## What's happening

- Data pipeline from seeding to deduplication
- Support for multiple data sources (clinics, hospitals, urgent care, etc.)
- CSV output of matched records with confidence scores

## Potential AWS Data Engineer learning opportunities

[Exam Guide for reference](https://d1.awsstatic.com/training-and-certification/docs-data-engineer-associate/AWS-Certified-Data-Engineer-Associate_Exam-Guide.pdf)

Different parts of this project can likely be adapted into the AWS environment to practice concepts covered in the AWS Data Engineer cert.

Here's are the sections I think may be most relevant:
- Perform data ingestion (src/mdp_dedupe/scripts/data_extraction)
- Creation of ETL pipelines (this is the basis of the project!)
- Orchestrate data pipelines (moving this project into AWS will be "orchestrating" a pipeline)
- Structuring SQL queries to meet data pipeline requirements
- Applying storage services to appropriate use cases

Overall, I think the code has immediate applications to the above but can be extended to explore all the concepts that are covered in the exam guide.

## Installation

1. Clone the repository:
```bash
git clone https://github.com/itskitto/mdp-dedupe.git
cd mdp-dedupe
```

2. Install dependencies:
```bash
poetry install
```

## Database Setup

This project uses PostgreSQL

Configure Environment:
```bash
cp .env.example .env

# Edit .env with your settings
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mdp_dedupe
DB_USER=your_username
DB_PASSWORD
```

## Usage

```bash
# Run the complete pipeline (seed -> extract -> preprocess -> dedupe)
poetry run mdp-dedupe --all

# Run individual steps
poetry run mdp-dedupe --seed        # Seed the database with test data
poetry run mdp-dedupe --extract     # Extract data from database
poetry run mdp-dedupe --preprocess  # Preprocess the data
poetry run mdp-dedupe --dedupe      # Run deduplication (default if no args)

# Configure seeding
poetry run mdp-dedupe --seed --pool-size 20 --duplicates 10 --unique 10

```

### Pipeline Steps

1. **Database Seeding** (`--seed`):
   - Creates database tables for each "separate" data source
   - Generates a shared pool of patient records (shared to make sure we create duplicates)
   - Seeds tables with controlled duplicates and unique records
   - If you want more data:
     - `--pool-size`: Number of shared patient records (default: 10)
     - `--duplicates`: Duplicates per table (default: 5)
     - `--unique`: Unique records per table (default: 5)

2. **Data Extraction** (`--extract`):
   - Extracts records from database tables
   - Applies initial field mappings (part of the process for standardizing the data for use across the code)
   - Saves to parquet files:
     - `data/processed/parquet/{source}_cleaned.parquet`

3. **Data Preprocessing** (`--preprocess`):
   - Normalizes fields:
     - Dates to standard format
     - Phone numbers stripped to digits
     - Addresses flattened and standardized
     - Text fields cleaned and lowercased
   - Saves to parquet files:
     - `data/processed/parquet/{source}_preprocessed.parquet`

4. **Deduplication** (`--dedupe`):
   - Loads preprocessed data
   - Trains or loads existing dedupe model
   - Outputs results to CSV:
     - `data/results/dedupe_results.csv`

## Database Schema

The project mocks a healthcare network that has multiple facilities that act as diverging data sources

### Clinic Patients
- patient_id (PK)
- first_name
- last_name
- date_of_birth
- phone_number
- email
- address
- insurance_id

### Urgent Care Patients
- patient_id (PK)
- first_name
- last_name
- dob
- phone
- email
- address_line
- insurance_id
- emergency_contact_name
- emergency_contact_phone

### Hospital Patients
- patient_id (PK)
- first_name
- middle_name
- last_name
- date_of_birth
- phone_number
- email_address
- address (JSON)
- insurance_provider
- policy_number

### Physical Therapy Patients
- patient_id (PK)
- full_name
- dob
- contact_phone
- email
- street_address
- city
- state
- zip_code
- insurance
