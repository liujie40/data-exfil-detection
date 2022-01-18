"""
Test the bq_setup script which creates the required
big query datasets and tables
"""
import logging
import pytest
import subprocess

from google.cloud import bigquery


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
dataset_args = full_args[:full_args.index("-t")]


@pytest.mark.parametrize("cli_args", [dataset_args])
@pytest.mark.usefixtures("bq_setup_script_teardown")
@pytest.mark.parametrize("bq_setup_script_teardown", [dataset_args], indirect=True)
def test_lanl_netflow_dataset_exists(session: bigquery.Client, cli_args) -> None:
    """
    Tests if the lanl_netflow dataset exists
    """
    logger.debug("Command passed to subprocess:\n%s", " ".join(["python"] + cli_args))
    subprocess.run(["python"] + cli_args)
    
    lanl_dataset: bigquery.dataset.Dataset = session.get_dataset("test_lanl_netflow")
    
    assert lanl_dataset



