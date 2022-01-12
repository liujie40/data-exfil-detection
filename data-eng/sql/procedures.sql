CREATE OR REPLACE PROCEDURE lanl_netflow.get_device_frequencies ()
BEGIN
  CREATE TEMP TABLE _device_freq (
    Device STRING,
    `Count` INTEGER
  );

  INSERT INTO _device_freq
  SELECT Device, COUNT(1) as `Count` FROM (
    SELECT DstDevice as Device FROM lanl_netflow.netflow
    UNION ALL
    SELECT SrcDevice as Device FROM lanl_netflow.netflow
  )
  GROUP BY Device
  ORDER BY `Count` DESC;
END;

CREATE OR REPLACE PROCEDURE lanl_netflow.create_strata()
BEGIN
  CREATE TEMP TABLE _device_strata (
    Device STRING,
    Strata STRING
  );
  
  INSERT INTO _device_strata
  SELECT Device, `Count` / count(`Count`)
  FROM _device_freq
  ORDER BY Count Desc
  
  
END;