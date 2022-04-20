from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="dit-m1ia-credentials.json"

# BigQuery Parameters
PROJECT_ID = "dit-m1ia"
LOCATION = "eu"
DATASET_ID = "ml_dataset"
TABLE_ID = "weather"
CSV_FILE = "weather_dataset_clean.csv"

# Create a client instance for your project
client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

# Create dataset
dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
dataset.location = LOCATION

dataset = client.create_dataset(dataset, timeout=30)
print(f"Created dataset {client.project}.{dataset.dataset_id}")

# Create table
client.create_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

dataset_ref = client.dataset(DATASET_ID)
table_ref = dataset_ref.table(TABLE_ID)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.autodetect = True

# Load data from csv file to BigQuery table
with open(CSV_FILE, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

# Waits for table load to complete
job.result()

# Looks like everything worked :)
print(f"Loaded {job.output_rows} rows into {DATASET_ID}:{TABLE_ID}.")