DROP TABLE IF EXISTS tr.median_temperatures_city;

CREATE TABLE tr.median_temperatures_city AS

SELECT
  s.name,
  d.id,
  year_month_day,
  SUM(case when element='TMAX' or element='TMIN' Then data_value Else NULL END)*1.0/2 as median_temp
FROM
  tr.yearly_data d
  JOIN tr.stations_5km s using(id)
GROUP BY  s.name,
          d.id,
          year_month_day;
