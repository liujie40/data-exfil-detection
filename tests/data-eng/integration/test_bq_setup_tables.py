"""
Test the bq_setup script which creates the required
big query tables
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
table_args = full_args[:full_args.index("-t") + 3]


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [(table_args, table_args), (table_args[:6], table_args[:6])],
    indirect=True
)
def test_lanl_netflow_table_exists(session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]) -> None:
    """
    Tests if the lanl_netflow dataset exists
    """
    subprocess.run(["python"] + bq_setup_cli_args)
    
    lanl_netflow_table: bigquery.table.Table = session.get_table("test_lanl_netflow.netflow")
    
    assert lanl_netflow_table
    

@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [(table_args, table_args), (table_args[:5] + table_args[6:7], table_args[:5] + table_args[6:7])],
    indirect=True
)
def test_test_data_netflow_table_exists(session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]) -> None:
    """
    Tests if the lanl_netflow dataset exists
    """
    subprocess.run(["python"] + bq_setup_cli_args)
    
    test_data_netflow_table: bigquery.table.Table = session.get_table("test_test_data.netflow")
    
    assert test_data_netflow_table