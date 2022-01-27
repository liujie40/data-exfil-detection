"""
Unit test the create_strata stored procedure to ensure
it creates strata based on the number of netflow communications
the devices are involved in

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
        WHERE routine_name = 'create_strata'
    """
    )
    results: pd.DataFrame = query.result().to_dataframe()

    expected = pd.Series(["create_strata"], name="routine_name")

    logger.debug("Results:\n%s", results)
    logger.debug("Results name:\n%s", results["routine_name"])
    logger.debug("Expected name:\n%s", expected)

    pd.testing.assert_series_equal(results["routine_name"], expected)


def test_create_strata_stratifies(session: bigquery.Client) -> None:
    """
    Check that stored procedure splits the data into
    separate strata
    """
    query: bigquery.job.QueryJob = session.query(
        """
        CALL test_data.create_strata();
        
        SELECT * FROM _device_strata;
    """
    )
    results: pd.DataFrame = query.result().to_dataframe()

    expected: bigquery.job.QueryJob = session.query(
        """
        SELECT Device, `Count` FROM test_data._device_strata;
    """
    )
    expected_results: pd.DataFrame = expected.result().to_dataframe()

    logger.debug("Results:\n%s", results)
    logger.debug("Expected strata:\n%s", expected_results)

    pd.testing.assert_frame_equal(results, expected_results)
