------------------------------------------
-- Stratify by number of communications --
------------------------------------------

SELECT DISTINCT SrcDevice
FROM `data-exfil-detection.lanl-netflow.netflow`
UNION ALL
SELECT DISTINCT DstDevice
FROM `data-exfil-detection.lanl-netflow.netflow`
