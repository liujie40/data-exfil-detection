"""
Test the bq_setup script which creates the required
big query datasets and tables
"""
import logging
import pytest
import subprocess

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import List


logger = logging.getLogger(__name__)


full_args = [
    "data-eng/setup/bq_setup.py",
    "data-exfil-detection",
    "-d",
    "test_lanl_netflow",
    "test_test_data",
    "-t",
    "test_lanl_netflow.netflow",
    "test_test_data.netflow",
    "test_test_data.device_level_data",
    "test_test_data._device_frequencies",
    "test_test_data._device_strata"
]


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [(full_args, full_args)],
    indirect=True
)
def test_datasets_and_tables_created(session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]) -> None:
    """
    Tests if the lanl_netflow dataset exists
    """
    subprocess.run(["python"] + bq_setup_cli_args)
    
    lanl_netflow_dataset: bigquery.dataset.Dataset = session.get_dataset("test_lanl_netflow")
    test_data_dataset: bigquery.dataset.Dataset = session.get_dataset("test_test_data")
    
    lanl_netflow_table: bigquery.table.Table = session.get_table("test_lanl_netflow.netflow")
    test_data_netflow_table: bigquery.table.Table = session.get_table("test_test_data.netflow")
    test_data_dld_table: bigquery.table.Table = session.get_table("test_test_data.device_level_data")
    test_data_device_freq_table: bigquery.table.Table = session.get_table("test_test_data._device_frequencies")
    test_data_device_strata_table: bigquery.table.Table = session.get_table("test_test_data._device_strata")

    ## Datasets
    assert lanl_netflow_dataset
    assert test_data_dataset
    
    ## Tables
    assert lanl_netflow_table
    assert test_data_netflow_table
    assert test_data_dld_table
    assert test_data_device_freq_table
    assert test_data_device_strata_table
    
    ## Schemas?
