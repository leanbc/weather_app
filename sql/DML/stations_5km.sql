DROP TABLE IF EXISTS tr.stations_5km;

CREATE TABLE tr.stations_5km AS

SELECT
  st.id,
  s.ascii as state,
  st.state as state_st,
  s.name,
  111.111 *
  DEGREES(ACOS(LEAST(1.0, COS(RADIANS(latitude))
                            * COS(RADIANS(lat))
                            * COS(RADIANS(longitude - lon))
    + SIN(RADIANS(latitude))
                            * SIN(RADIANS(lat))))) AS distance_in_km

FROM tr.stations st
       FULL JOIN tr.stadt s on s.typ='Stadt'
Where
    id like 'GM%'
  AND 111.111 * DEGREES(ACOS(LEAST(1.0, COS(RADIANS(latitude))
           * COS(RADIANS(lat))
           * COS(RADIANS(longitude - lon))
           + SIN(RADIANS(latitude))
           * SIN(RADIANS(lat))))) < 5.0
;
