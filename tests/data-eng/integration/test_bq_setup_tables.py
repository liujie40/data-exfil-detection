"""
Test the bq_setup script which creates the required
big query tables

Author: Daniel Yates
"""
import logging
import subprocess

from typing import List, Dict, Any, Optional

import pytest

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


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
    "test_test_data._device_strata",
]
table_args = full_args[: full_args.index("-t") + 3]


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [(table_args, table_args), (table_args[:7], table_args[:7])],
    indirect=True,
)
def test_lanl_netflow_table_exists(
    session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]
) -> None:
    """
    Tests if the lanl_netflow table exists
    """
    # pylint: disable=unused-argument
    subprocess.run(["python"] + bq_setup_cli_args, check=True)

    lanl_netflow_table: bigquery.table.Table = session.get_table(
        "test_lanl_netflow.netflow"
    )

    assert lanl_netflow_table


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [
        (table_args, table_args),
        (table_args[:6] + table_args[7:8], table_args[:6] + table_args[7:8]),
    ],
    indirect=True,
)
def test_test_data_netflow_table_exists(
    session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]
) -> None:
    """
    Tests if the lanl_netflow table exists
    """
    # pylint: disable=unused-argument
    subprocess.run(["python"] + bq_setup_cli_args, check=True)

    test_data_netflow_table: bigquery.table.Table = session.get_table(
        "test_test_data.netflow"
    )

    assert test_data_netflow_table


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [
        (table_args[: len(table_args) - 3], table_args[: len(table_args) - 3]),
        (table_args[: len(table_args) - 2], table_args[: len(table_args) - 2]),
    ],
    indirect=True,
)
def test_no_tables_passed_none_created(
    session: bigquery.Client, bq_setup_script_teardown, bq_setup_cli_args: List[str]
) -> None:
    """
    Test that no tables are created if there aren't any passed as args
    """
    # pylint: disable=unused-argument
    subprocess.run(["python"] + bq_setup_cli_args, check=True)

    test_lanl_netflow_exists: Optional[bool] = None
    test_test_data_netflow_exists: Optional[bool] = None

    try:
        session.get_table("test_lanl_netflow.netflow")
        test_lanl_netflow_exists = True
    except NotFound:
        test_lanl_netflow_exists = False

    try:
        session.get_table("test_test_data.netflow")
        test_test_data_netflow_exists = True
    except NotFound:
        test_test_data_netflow_exists = False

    assert not all([test_lanl_netflow_exists, test_test_data_netflow_exists])


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args",
    [
        (table_args[: len(table_args) - 3], table_args[: len(table_args) - 3]),
        (table_args[: len(table_args) - 2], table_args[: len(table_args) - 2]),
    ],
    indirect=True,
)
def test_no_tables_passed_warning_raised(
    bq_setup_script_teardown, bq_setup_cli_args: List[str]
) -> None:
    """
    Tests that the script logs a warning that no table args
    were passed
    """
    # pylint: disable=unused-argument
    c_p: subprocess.CompletedProcess = subprocess.run(
        ["python"] + bq_setup_cli_args,
        capture_output=True,
        encoding="utf-8",
        check=True,
    )

    assert "No tables passed to script" in vars(c_p)["stderr"]


@pytest.mark.parametrize(
    "bq_setup_script_teardown, bq_setup_cli_args, get_schema",
    [
        (table_args, table_args, "data-eng/schemas/netflow.json"),
        (table_args[:7], table_args[:7], "data-eng/schemas/netflow.json"),
    ],
    indirect=True,
)
def test_table_created_with_correct_schema(
    session: bigquery.Client,
    bq_setup_script_teardown,
    bq_setup_cli_args: List[str],
    get_schema: Dict[str, Any],
) -> None:
    """
    Tests that the tables are created with the correct schema
    """
    # pylint: disable=unused-argument
    subprocess.run(["python"] + bq_setup_cli_args, check=True)

    lanl_netflow_table: bigquery.table.Table = session.get_table(
        "test_lanl_netflow.netflow"
    )

    actual_schema: Dict[str, Any] = lanl_netflow_table.to_api_repr()["schema"]["fields"]
    expected_schema = get_schema

    assert actual_schema == expected_schema
