DROP TABLE IF EXISTS tr.max_temp;

CREATE TABLE tr.max_temp AS

SELECT
    name,
    year_month_day,
    data_value
FROM tr.yearly_data 
    JOIN tr.stations_5km USING (id)
Where 
    element='TMAX'