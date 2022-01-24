"""
Fixtures to be used in SQL unit tests

Author: Daniel Yates
"""
import json
import logging
import pathlib
import pytest

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import List, Dict, Any, Union

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def get_schema(request) -> Dict[str, Any]:
    schema_path: Union[pathlib.Path, str] = request.param
    logger.debug("Schema path provided: %s", schema_path)

    if isinstance(schema_path, pathlib.Path) or isinstance(schema_path, str):
        with open(schema_path, "r") as schema_file:
            return json.load(schema_file)
    else:
        raise TypeError("%s is not a string or a pathlib.Path")

@pytest.fixture(scope="function")
def bq_setup_cli_args(request) -> List[str]:
    logger.debug(
        "Command passed to subprocess:\n%s",
        " ".join(["python"] + request.param)
    )
    
    yield request.param


@pytest.fixture(scope="function")
def bq_setup_script_teardown(request, session: bigquery.Client) -> None:
    yield
    
    try:
        start = request.param.index("-d")
    except ValueError:
        logger.warning("No datasets in arguments")
        return None
    
    try:
        end = request.param.index("-t")
    except ValueError:
        end = len(request.param)
    
    for dataset in request.param[start + 1: end]:
        try:
            session.delete_dataset(dataset, delete_contents=True)
            logger.info("%s deleted", dataset)
        except NotFound:
            logger.warning("%s dataset not found", dataset)
            

@pytest.fixture(scope="session")
def session() -> bigquery.Client:
    """
    Creates a bigquery connection to execute sql queries.
    Closes the connection in the tear down
    """
    client = bigquery.Client(location="EU")
    logger.info("Session created")

    yield client

    client.close()
    logger.info("Session closed")


@pytest.fixture(scope="session")
def create_stored_procedures(session: bigquery.Client) -> None:
    """
    Runs all of the SQL statements to create the stored procedures.
    """
    logger.info("Creating stored procedures")

    with open("data-eng/sql/procedures.sql", "r", encoding="utf-8") as proc_file:
        procedures = proc_file.read()

    procs_processed = 0
    for proc in procedures.split("END;"):
        if proc.replace("\n", ""):
            proc = proc.replace("lanl_netflow.", "test_data.")
            proc += "END;"

            logger.debug("Procedure being ran:\n%s", proc)

            query: bigquery.job.QueryJob = session.query(proc)
            query.result()

            procs_processed += 1

    logger.info("%d statements executed", procs_processed)
