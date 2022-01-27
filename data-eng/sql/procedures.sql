CREATE OR REPLACE PROCEDURE lanl_netflow.get_device_frequencies ()
BEGIN
  CREATE TEMP TABLE _device_freq (
    Device STRING,
    `Count` INTEGER
  );

  INSERT INTO _device_freq
  SELECT Device, COUNT(1) AS `Count` FROM (
    SELECT DstDevice AS Device FROM lanl_netflow.netflow
    UNION ALL
    SELECT SrcDevice AS Device FROM lanl_netflow.netflow
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
  SELECT
    Device,
    CASE
      WHEN CumProps < 0.33 THEN "High"
      WHEN CumProps < 0.66 AND CumProps >= 0.33 THEN "Medium"
      WHEN CumProps >= 0.66 THEN "Low"
    END AS Strata
  FROM (
    SELECT
      Device,
      SUM(Proportions) OVER(ORDER BY Proportions DESC, Device DESC) AS CumProps
    FROM(
      SELECT
        Device,
        `Count` / (
          SELECT SUM(`Count`) FROM _device_freq
        ) AS Proportions
      FROM _device_freq
      ORDER BY Count Desc
    )
  );
  
END;
