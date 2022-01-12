"""
Unit test the stored procedures to ensurte they are performing their intended
function.

Author: Daniel Yates
"""
from google.cloud import bigquery
import logging
import pandas as pd
import pytest


logger = logging.getLogger(__name__)


## ###################### ##
## GET_DEVICE_FREQUENCIES ##
## ###################### ##
@pytest.mark.usefixtures("create_stored_procedures")
def test_get_device_frequencies_exists(session):
    q = session.query(
        """
        SELECT *
        FROM `data-exfil-detection.test_data.INFORMATION_SCHEMA.ROUTINES`
        WHERE routine_name = 'get_device_frequencies'
    """
    )
    results = q.result().to_dataframe()

    expected = pd.Series(["get_device_frequencies"], name="routine_name")

    logger.debug(f"Results:\n{results}")
    logger.debug(f"Results name:\n{results['routine_name']}")
    logger.debug(f"Expected name:\n{expected}")

    pd.testing.assert_series_equal(results["routine_name"], expected)


def test_get_device_frequencies_calculate_frequencies(session):
    q = session.query(
        """
        CALL test_data.get_device_frequencies()
    """
    )
    results = q.result().to_dataframe()

    expected = pd.DataFrame(
        {"Device": ["Device3", "Device1", "Device2"], "Count": [17, 15, 10]}
    )

    logger.debug(f"Results:\n{results}")
    logger.debug(f"Expected:an{expected}")

    pd.testing.assert_frame_equal(results, expected)
