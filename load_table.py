from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "finance-cloud-team.LAHU.APPLICATION_DATA_21DEC2023"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    skip_leading_rows=1
    )
uri = "gs://fotclaml-data-01/input/application_data.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Wait for the job to complete.

table = client.get_table(table_id)
print("Loaded {} rows to table {}".format(table.num_rows, table_id))
