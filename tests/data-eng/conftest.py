from google.cloud import bigquery
import logging
import pytest


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def session():
    client = bigquery.Client(location="EU")
    logging.info("Session created")

    yield client

    client.close()
    logging.info("Session closed")


@pytest.fixture(scope="session")
def create_stored_procedures(session):
    logging.info("Creating stored procedures")

    with open("data-eng/sql/procedures.sql", "r") as f:
        procedures = f.read()

    procs_processed = 0
    for proc in procedures.split("END;"):
        if proc.replace("\n", ""):
            proc = proc.replace("lanl_netflow.", "test_data.")
            proc += "END;"

            logging.debug(f"Procedure being ran:\n{proc}")

            q = session.query(proc)
            query_job = q.result()

            procs_processed += 1

    logging.info(f"{procs_processed} statements executed")
