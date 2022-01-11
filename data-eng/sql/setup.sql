----------------------------------------------------------------------
-- TODO:                                                            --
--     * Check bigquery types and update table creation if required --
--     * Finish device strata query                                 --
----------------------------------------------------------------------

------------------------------------------
-- Stratify by number of communications --
------------------------------------------

CREATE TEMPORARY TABLE _device_frequencies (
  device STRING,
  freq INTEGER
)

CREATE TEMPORARY TABLE _device_strata (
  device STRING,
  strata SMALLINT
)

INSERT INTO _device_frequencies
SELECT device, COUNT(1) as freq
FROM (
  SELECT SrcDevice as device
  FROM `data-exfil-detection.lanl-netflow.netflow`
  UNION ALL
  SELECT DstDevice as device
  FROM `data-exfil-detection.lanl-netflow.netflow`
) t1
GROUP BY device
ORDER BY freq DESC



SELECT
  device,
  SUM(proportions)
    OVER (
      ORDER BY proportions DESC
    ) AS cum_proportions
FROM (
  SELECT device, freq / SUM(freq) AS proportions
  FROM device_frequencies
)
