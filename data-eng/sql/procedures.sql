CREATE OR REPLACE PROCEDURE lanl_netflow.get_device_frequencies ()
BEGIN
  SELECT Device, COUNT(1) as Count FROM (
    SELECT DstDevice as Device FROM lanl_netflow.netflow
    UNION ALL
    SELECT SrcDevice as Device FROM lanl_netflow.netflow
  )
  GROUP BY Device
  ORDER BY Count DESC;
END;