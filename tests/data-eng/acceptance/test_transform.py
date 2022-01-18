"""
Test that setup.sql transforms netflow data to device level network data

Author: Daniel Yates
"""
import logging
import pandas as pd
import pytest

from google.cloud import bigquery.Client
from google.cloud import bigquery.job.QueryJob


logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("create_stored_procedures")
def test_data_is_transformed(session: bigquery.Client) -> None:
    """
    Acceptance test to ensure the transform sql script
    transforms netflow data to device level network data
    """
    with open("data-eng/sql/transform.sql", "r", encoding="utf-8") as transform_file:
        script = transform_file.read()

    script = script.replace(
        "`data-exfil-detection.lanl_netflow.netflow`",
        "`data-exfil-detection.test_data.netflow`",
    )

    logger.debug("Script being ran:\n%s", script)

    query: bigquery.job.QueryJob = session.query(script)
    query.result()

    transformed_data: pd.DataFrame = (
        session.query(
            """
        SELECT * FROM `data-exfil-detection.test_data.transformed_data`
        """
        )
        .result()
        .to_dataframe()
    )
    logger.debug("Transformed_data:\n%s", transformed_data)

    test_data: pd.DataFrame = (
        session.query(
            """
        SELECT * FROM `data-exfil-detection.test_data.device_level_data`
        """
        )
        .result()
        .to_dataframe()
    )
    logger.debug("Test_data:\n%s", test_data)

    pd.testing_assert_frame_equal(transformed_data, test_data)
