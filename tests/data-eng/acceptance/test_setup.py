"""
Test that setup.sql transforms netflow data to device level network data

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
def test_data_is_transformed(session):
    with open("data-eng/setup.sql", "r") as f:
        script = f.read()
    
    logging.debug(f"Script being ran:\n{script}")
    
    q = session.query(script)
    query_job = q.result()
    
    transformed_data = session.query(
        """
        SELECT * FROM `data-exfil-detection.test_data.transformed_data`
        """
    ).result().to_dataframe()
    logging.debug(f"Transformed_data:\n{transformed_data}")
    
    test_data = session.query(
        """
        SELECT * FROM `data-exfil-detection.test_data.device_level_data`
        """
    ).result().to_dataframe()
    logging.debug(f"Test_data:\n{test_data}")
    
    pd.testing_assert_frame_equal(transformed_data, test_data)
