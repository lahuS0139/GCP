import base64
from google.cloud import bigquery

def load_data():
  client = bigquery.Client() # Construct a BigQuery client object.
  table_id = "finance-cloud-team.LAHU.APPLICATION_DATA_21DEC2023" # TODO(developer): Set table_id to the ID of the table to create.
  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourcFormat.CSV,autodetect=True,skip_leading_rows=1)
  uri = "gs://fotclaml-data-01/input/application_data.csv"
  load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.
  load_job.result()  # Wait for the job to complete.
  table = client.get_table(table_id)
  print("Loaded {} rows to table {}".format(table.num_rows, table_id))

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    load_data()

if __name__ == "__main__":
    hello_pubsub('data', 'context')
