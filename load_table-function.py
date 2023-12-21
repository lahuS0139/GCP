import functions_framework
from google.cloud import bigquery
def loadtable(data):
  # Construct a BigQuery client object.
  client = bigquery.Client()
  
  # TODO(developer): Set table_id to the ID of the table to create.
  table_id = "finance-cloud-team.LAHU.APPLICATION_DATA_21DEC2023_1"
  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourcFormat.CSV,autodetect=True,skip_leading_rows=1)
  uri = "gs://fotclaml-data-01/input/application_data.csv"
  load_job = client.load_table_from_uri(uri,table_id,job_config=job_config)  # Make an API request.
  load_job.result()  # Wait for the job to complete.
  table = client.get_table(table_id)
  print("Loaded {} rows to table {}".format(table.num_rows, table_id))

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
  data = cloud_event.data
  event_id = cloud_event["id"]
  event_type = cloud_event["type"]
  bucket = data["bucket"]
  name = data["name"]
  metageneration = data["metageneration"]
  timeCreated = data["timeCreated"]
  updated = data["updated"]
  print(f"Event ID: {event_id}")
  print(f"Event type: {event_type}")
  print(f"Bucket: {bucket}")
  print(f"File: {name}")
  print(f"Metageneration: {metageneration}")
  print(f"Created: {timeCreated}")
  print(f"Updated: {updated}")
loadtable(data)