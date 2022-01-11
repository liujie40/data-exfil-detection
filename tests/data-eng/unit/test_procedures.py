"""
Unit test the stored procedures to ensurte they are performing their intended
function.

Author: Daniel Yates
"""
from google.cloud import bigquery
import logging
import pandas as pd
import pytest


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def session():
    client = bigquery.Client()
    logging.info("Session created")
    
    yield client
    
    client.close()
    logging.info("Session closed")


@pytest.fixture(scope="session")
def create_stored_procedures(session):
    logging.info("Creating stored procedures")
    
    with open("data-eng/procedures.sql", "r") as f:
        procedures = f.read()
    
    procs_processed = 0
    for proc in procedures.split(";"):
        if proc.replace("\n", ""):
            logging.debug(f"Procedure being ran:\n{proc}")
            
            q = session.query(proc)
            query_job = q.result()
            
            procs_processed += 1
    
    logging.info(f"{procs_processed} statements executed")
    
    
@pytest.mark.usefixtures("create_stored_procedures")
def test_get_device_frequencies_exists(session):
    q = session.query(
        """SELECT *
           FROM `data-exfil-detection.lanl_netflow.INFORMATION_SCHEMA.ROUTINES`
           WHERE routine_name = 'test_proc'"""
        )
    results = q.result().to_dataframe()
    
    logging.debug(f"Results:\n{results}")
    logging.debug(f"Attribute name:\n{results['routine_name']}")
    logging.debug(f"Test name:\n{pd.Series(['test_proc'])}")
    
    pd.testing.assert_series_equal(
        results["routine_name"], pd.Series(["test_proc"], name="routine_name")
    )
