"""
Script to create the required bigquery datasets and tables.

Author: Daniel Yates
"""
import argparse
import logging

from google.cloud import bigquery
from typing import Dict, List, Optional


logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(
    description="Create bigquery datasets and tables"
)

parser.add_argument(
    "-d",
    "--dataset",
    nargs="*",
    help="The name of the dataset(s) to be created"
)
# parser.add_argument(
#     "-t",
#     "--table",
#     nargs="*",
#     help="The name of the table(s) to be created",
#     dest="table"
# )

args: Dict[str, List[str]] = vars(parser.parse_args())

client = bigquery.Client(location="EU")
logger.info("Session created")

for dataset in args["dataset"]:
    client.create_dataset(dataset)

client.close()
logger.info("Session closed")
