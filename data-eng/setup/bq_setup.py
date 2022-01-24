"""
Script to create the required bigquery datasets and tables.

Author: Daniel Yates
"""
import argparse
import logging
import json

from google.cloud import bigquery
from typing import Dict, List, Optional


logger = logging.getLogger(__name__)
parser = argparse.ArgumentParser(description="Create bigquery datasets and tables")

parser.add_argument("project", type=str, help="The project name on GCP")
parser.add_argument(
    "-d", "--dataset", nargs="*", help="The name of the dataset(s) to be created"
)
parser.add_argument(
    "-t",
    "--table",
    nargs="*",
    help="The name of the table(s) to be created",
    dest="table",
)

args: Dict[str, List[str]] = vars(parser.parse_args())

client = bigquery.Client(location="EU")
logger.info("Session created")

logger.debug("Project: %s", args["project"])

## ######## ##
## Datasets ##
## ######## ##
logger.debug("Dataset(s): %s", args["dataset"])

if not args["dataset"]:
    logger.warning("No datasets passed to script")
else:
    for dataset in args["dataset"]:
        logger.info("Creating dataset %s", dataset)
        client.create_dataset(dataset)

## ###### ##
## Tables ##
## ###### ##
logger.debug("Table(s): %s", args["table"])

if not args["table"]:
    logger.warning("No tables passed to script")
else:
    for table in args["table"]:
        logger.info("Creating table %s", table)

        with open(f"data-eng/schemas/{table.split('.')[1]}.json", "r") as schema_file:
            schema = json.load(schema_file)

        table = bigquery.table.Table(f"{args['project']}." + table, schema)

        client.create_table(table)

client.close()
logger.info("Session closed")
