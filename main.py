from google.cloud import bigquery
from datetime import datetime

def load_data_from_bucket(event, context):
    # Extract event data
    event_data = event
    print(f"EVENT DATA : {event_data}")

    # Construct file path from event data
    filepath = f"gs://{event_data['bucket']}/{event_data['name']}"
    print(f"FILE PATH : {filepath}")

    # Extract file name from event data
    filename = event_data["name"]
    print(f"FILE NAME : {filename}")

    # Construct table name from file name
    tablename = filename.rstrip(".csv").upper()
    print(f"TABLE NAME : {tablename}")

    # Get the current date and time
    current_datetime = datetime.now()  
    # Format the date and time
    formatted_datetime = current_datetime.strftime("%Y%m%d_%H%M%S")  

    # Construct table ID using project, dataset, and table name
    table_id = f"finance-cloud-team.LAHU.{tablename}_{formatted_datetime}"

    # Construct a BigQuery client object
    client = bigquery.Client()

    # Configure job to load data from CSV file
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, 
        autodetect=True, 
        skip_leading_rows=1
    )

    # Making an API request to load data from the file URI to the table
    load_job = client.load_table_from_uri(filepath, table_id, job_config=job_config)
    load_job.result()  # Waiting for the job to complete

    # Retrieve table details and print the number of loaded rows
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows to table {table_id}")
