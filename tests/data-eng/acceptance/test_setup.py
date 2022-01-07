"""
Test that setup.sql transforms netflow data to device level network data

Author: Daniel Yates
"""
from google.cloud import bigquery
import pytest


@pytest.fixture(scope="session")
def create_stored_procedures():
    client = bigquery.Client()
    
    with open("data-eng/procedures.sql", r) as f:
        procedures = f.read()
    
    for proc in procedures.split(";")
        q = client.query(proc)
        query_job = q.result()
    
    return client
