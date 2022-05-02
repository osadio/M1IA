from google.cloud import bigquery
import argparse
import os
import sys


def parse_command_line_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_id',
        type=str)
    parser.add_argument(
        '--cloud_region',
        type=str)
    parser.add_argument(
        '--dataset_id',
        type=str)
    parser.add_argument(
        '--table_id',
        type=str)
    parser.add_argument(
        '--csv_file',
        type=str)
    parser.add_argument(
        '--credentials_file',
        type=str)
    return parser.parse_args()


def set_credentials(credentials_file):
    # Set GCP credentials in OS environment variable
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file


def create_bq_dataset(client, project_id, dataset_id, cloud_region):
    # Create Big Query dataset
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.cloud_region = cloud_region
    return client.create_dataset(dataset, timeout=30)


def create_bq_table(client, project_id, dataset_id, table_id):
    # Create Big Query table
    return client.create_table(f"{project_id}.{dataset_id}.{table_id}")


def load_bq_data(client, dataset_id, table_id, csv_file):
    # Load data from csv file to Big Query table
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        # Waits for table load to complete
        job.result()
        return job

def main(argv):
    # Get arguments
    args = parse_command_line_args()
    project_id = args.project_id
    cloud_region = args.cloud_region
    dataset_id = args.dataset_id
    table_id = args.table_id
    csv_file = args.csv_file
    credentials_file = args.credentials_file

    # GCP credentials
    set_credentials(credentials_file)

    # Create a client instance for your project
    client = bigquery.Client(project=project_id, location=cloud_region)

    # Create bq dataset
    dataset = create_bq_dataset(client, project_id, dataset_id, cloud_region)
    if dataset :
        print(f"Created dataset {dataset.project}.{dataset.dataset_id}")
    else :
        print(f"Error during dataset {dataset.project}.{dataset.dataset_id} creation")
        quit()

    # Create bq table
    table = create_bq_table(client, project_id, dataset_id, table_id)
    if table :
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
    else :
        print(f"Error during table {table.project}.{table.dataset_id}.{table.table_id} creation")
        quit()

    # Load data to bq table
    print(f"Loading data from {csv_file} file...")
    job = load_bq_data(client, dataset_id, table_id, csv_file)

    if (job and job.output_rows):
        print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}")
    else :
        print(f"Error during data loading from {csv_file} file")

    
if __name__ == '__main__':
    main(sys.argv)



