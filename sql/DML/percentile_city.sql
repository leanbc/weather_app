DROP TABLE IF EXISTS tr.percentile_city;

CREATE TABLE tr.percentile_city AS

With median_city as (

  SELECT
  left(state, strpos(state, '-') - 1) city,
  year_month_day,
  (MAX(data_value)+MIN(data_value))/2 as max_temp
FRom tr.yearly_data
  join tr.stations using (id)
Where element='TMAX'
and stations.id like 'GM%'
Group by city,year_month_day),

city_agg as (

SELECT city,
       year_month_day,
       max_temp,
       median_temp as median_temp_germany,
       ((max_temp-median_temp) >= 30)::INT  as hotness_day
FROM median_city
      LEFT JOIN tr.median_temperatures_germany using (year_month_day)
WHERE date(year_month_day::TEXT)>='20180501'::DATE
      and date(year_month_day::TEXT)<'20190501'::DATE),


final_table as (
SELECT city,
       SUM(hotness_day) as hotness_total
FROM city_agg
group by city
Order by hotness_total)


select final_table.city
     ,final_table.hotness_total
     , ntile(10) over (order by final_table.hotness_total)
from final_table;
