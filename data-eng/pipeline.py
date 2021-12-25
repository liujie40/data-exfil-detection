import apache_beam as beam
from apache_beam.io import fileio
import json
import os


with open("./data-eng/netflow_table_schema.json", "r") as f:
    table_schema = json.load(f)


table_spec = f"{os.environ['GCP_PROJECT']}:lanl_netflow.netflow_V2"

with beam.Pipeline() as pipeline:
  readable_files = (
      pipeline
      | fileio.MatchFiles(f"{os.environ['GCP_BUCKET_NAME']}/compressed/netflow_day-*.bz2")
      | fileio.ReadMatches())
  files_and_contents = (
      readable_files
      | beam.Map(lambda x: (x.metadata.path, x.read_utf8())))
  write_files = (
      files_and_contents
      | beam.io.WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=f"gs://{os.environ['GCP_BUCKET_NAME']}/tmp/"
        ))