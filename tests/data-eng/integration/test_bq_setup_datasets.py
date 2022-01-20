"""
Test the bq_setup script which creates the required
big query datasets
"""
import logging
import pytest
import subprocess

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict
from typing import List, Optional


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
table_args = full_args[:full_args.index("-t") + 3]


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [(dataset_args, dataset_args), (dataset_args[:3], dataset_args[:3])],
    indirect=True
)
def test_lanl_netflow_dataset_exists(session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]) -> None:
    """
    Tests if the lanl_netflow dataset exists
    """
    subprocess.run(["python"] + bq_setup_cli_args)
    
    lanl_dataset: bigquery.dataset.Dataset = session.get_dataset("test_lanl_netflow")
    
    assert lanl_dataset


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [(dataset_args, dataset_args), (dataset_args[:2] + dataset_args[3:4], dataset_args[:2] + dataset_args[3:4])],
    indirect=True
)
def test_test_data_dataset_exists(session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]) -> None:
    """
    Tests if the test_data dataset exists
    """
    subprocess.run(["python"] + bq_setup_cli_args)
    
    test_data: bigquery.dataset.Dataset = session.get_dataset("test_test_data")
    
    assert test_data
    

@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [(dataset_args[0:1], dataset_args[0:1]), (dataset_args[:2], dataset_args[:2])],
    indirect=True
)
def test_no_datasets_passed_none_created(session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]) -> None:
    """
    Tests that the script doesn't create any datasets when passed
    no dataset args
    """
    subprocess.run(["python"] + bq_setup_cli_args)
    
    test_lanl_netflow_exists: Optional[bool] = None
    test_test_data_exists: Optional[bool] = None
    
    try:
        session.get_dataset("test_lanl_netflow")
        test_lanl_netflow_exists = True
    except NotFound:
        test_lanl_netflow_exists = False
    
    try:
        session.get_dataset("test_test_data")
        test_test_data_exists = True
        except NotFound:
            test_test_data_exists = False  

    assert not all([test_lanl_netflow_exists, test_test_data_exists])


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [(dataset_args[0:1], dataset_args[0:1]), (dataset_args[:2], dataset_args[:2])],
    indirect=True
)
def test_no_datasets_passed_warning_raised(bq_setup_script_teardown, bq_setup_cli_args: List[str]) -> None:
    """
    Tests that the script logs a warning that no dataset args
    were passed
    """
    cp: subprocess.CompletedProcess = subprocess.run(
        ["python"] + bq_setup_cli_args,
        capture_output=True,
        encoding="utf-8"
    )
    
    assert "No datasets passed to script" in vars(cp)["stderr"]
    

@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [
        (dataset_args, dataset_args),
        (dataset_args[:3], dataset_args[:3]),
        (dataset_args[:2] + dataset_args[3:4], dataset_args[:2] + dataset_args[3:4])
    ],
    indirect=True
)
def test_dataset_already_exists_fails(session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]) -> None:
    """
    Tests that the dataset creation fails if the dataset already exists
    """
    start = bq_setup_cli_args.index("-d")
    end = len(bq_setup_cli_args)
    
    for dataset in bq_setup_cli_args[start + 1: end]:
        session.create_dataset(dataset)
        logger.debug("%s created", dataset)
    
    cp: subprocess.CompletedProcess = subprocess.run(
        ["python"] + bq_setup_cli_args,
        capture_output=True,
        encoding="utf-8"
    )
    conflict_exception = Conflict.__module__ + "." + Conflict.__qualname__
    
    assert conflict_exception in vars(cp)["stderr"]
    