"""
Test that setup.sql transforms netflow data to device level network data

Author: Daniel Yates
"""
from google.cloud import bigquery
import logging
import pandas as pd
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("create_stored_procedures")
def test_data_is_transformed(session):
    with open("data-eng/sql/transform.sql", "r") as f:
        script = f.read()

    script = script.replace(
        "`data-exfil-detection.lanl_netflow.netflow`",
        "`data-exfil-detection.test_data.netflow`",
    )

    logger.debug(f"Script being ran:\n{script}")

    q = session.query(script)
    query_job = q.result()

    transformed_data = (
        session.query(
            """
        SELECT * FROM `data-exfil-detection.test_data.transformed_data`
        """
        )
        .result()
        .to_dataframe()
    )
    logger.debug(f"Transformed_data:\n{transformed_data}")

    test_data = (
        session.query(
            """
        SELECT * FROM `data-exfil-detection.test_data.device_level_data`
        """
        )
        .result()
        .to_dataframe()
    )
    logger.debug(f"Test_data:\n{test_data}")

    pd.testing_assert_frame_equal(transformed_data, test_data)
