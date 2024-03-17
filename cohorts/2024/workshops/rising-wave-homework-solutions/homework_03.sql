WITH latest_pickup_time AS (
    SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time, MAX(tpep_pickup_datetime - interval '17 hours') as earlier_time
    FROM trip_data
)
SELECT tz.zone as pickup_zone, count(td.vendorid) as trips
FROM trip_data td
JOIN taxi_zone tz on td.pulocationid = tz.location_id
JOIN latest_pickup_time lpt ON td.tpep_pickup_datetime <= lpt.latest_pickup_time AND td.tpep_pickup_datetime > (lpt.latest_pickup_time - interval '17 hours')
GROUP BY 1
ORDER BY 2 DESC
LIMIT 3;