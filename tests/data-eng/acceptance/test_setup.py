"""
Test that setup.sql transforms netflow data to device level network data

Author: Daniel Yates
"""
from google.cloud import bigquery
import logging
import pandas as pd
import pytest


logger = logging.getLogger(__name__)


@pytest.fixture()
def session():
    client = bigquery.Client()
    yield client
    logging.info("Session created")
    client.close()
    logging.info("Session closed")


@pytest.fixture(scope="session")
def create_stored_procedures(session):
    logging.info("Creating stored procedures")
    
    with open("data-eng/procedures.sql", r) as f:
        procedures = f.read()
    
    procs_processed = 0
    for proc in procedures.split(";"):
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
    
    pd.testing.assert_series_equal(
        results["routine_name"], pd.Series(["test_proc"])
    )
