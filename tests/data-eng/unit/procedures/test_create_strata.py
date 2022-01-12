"""
Unit test the create_strata stored procedure to ensure
it creates strata based on the number of netflow communications
the devices are involved in

Author: Daniel Yates
"""
import logging
import pandas as pd
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("create_stored_procedures")
def test_get_device_frequencies_exists(session):
    """
    Check that stored procedure is available in the DB
    """
    query = session.query(
        """
        SELECT *
        FROM `data-exfil-detection.test_data.INFORMATION_SCHEMA.ROUTINES`
        WHERE routine_name = 'create_strata'
    """
    )
    results = query.result().to_dataframe()

    expected = pd.Series(["create_strata"], name="routine_name")

    logger.debug("Results:\n%s", results)
    logger.debug("Results name:\n%s", results["routine_name"])
    logger.debug("Expected name:\n%s", expected)

    pd.testing.assert_series_equal(results["routine_name"], expected)
    

def test_create_strata_stratifies(session):
    """
    Check that stored procedure splits the data into
    separate strata
    """
    query = session.query(
        """
        CALL test_data.create_strata();
        
        SELECT * FROM _device_strata;
    """
    )
    results = query.result().to_dataframe()
    
    expected = pd.DataFrame({
        "Device": ["Device3", "Device1", "Device2"],
        "Strata": ["high", "medium", "low"]
    })
    
    logger.debug("Results:\n%s", results)
    logger.debug("Expected strata:\n%s", expected)
    
    pd.testing.assert_frame_equal(results, expected)
