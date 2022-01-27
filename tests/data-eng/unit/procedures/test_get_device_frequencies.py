"""
Unit test the get_device_frequencies stored procedure to ensure
it calculates the frequencies of each device in the dataset

Author: Daniel Yates
"""
import logging
import pandas as pd
import pytest

from google.cloud import bigquery


logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("create_stored_procedures")
def test_get_device_frequencies_exists(session: bigquery.Client) -> None:
    """
    Check that stored procedure is available in the DB
    """
    query: bigquery.job.QueryJob = session.query(
        """
        SELECT *
        FROM `data-exfil-detection.test_data.INFORMATION_SCHEMA.ROUTINES`
        WHERE routine_name = 'get_device_frequencies'
    """
    )
    results: pd.DataFrame = query.result().to_dataframe()

    expected = pd.Series(["get_device_frequencies"], name="routine_name")

    logger.debug("Results:\n%s", results)
    logger.debug("Results name:\n%s", results["routine_name"])
    logger.debug("Expected name:\n%s", expected)

    pd.testing.assert_series_equal(results["routine_name"], expected)


def test_get_device_frequencies_calculate_frequencies(session: bigquery.Client) -> None:
    """
    Check that the number of each device is calculated
    """
    query: bigquery.job.QueryJob = session.query(
        """
        CALL test_data.get_device_frequencies();
        
        SELECT Device, Count FROM _device_freq
        ORDER BY Count DESC, Device DESC;
    """
    )
    results: pd.DataFrame = query.result().to_dataframe()

    expected: bigquery.job.QueryJob = session.query(
        """
        SELECT Device, `Count` FROM test_data._device_freq
        ORDER BY `Count` DESC, Device DESC;
    """
    )
    expected_results: pd.DataFrame = expected.result().to_dataframe()

    logger.debug("Results:\n%s", results)
    logger.debug("Expected:\n%s", expected_results)

    pd.testing.assert_frame_equal(results, expected_results)
 
