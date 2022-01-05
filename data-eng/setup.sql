------------------------------------------
-- Stratify by number of communications --
------------------------------------------

WITH device_frequencies AS (
  SELECT device, count(1) as freq
  FROM (
    SELECT DISTINCT SrcDevice as device
    FROM `data-exfil-detection.lanl-netflow.netflow`
    UNION ALL
    SELECT DISTINCT DstDevice as device
    FROM `data-exfil-detection.lanl-netflow.netflow`
  ) t1
  GROUP BY device
  ORDER BY freq DESC
)


