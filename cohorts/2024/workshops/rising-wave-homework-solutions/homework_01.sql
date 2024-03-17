CREATE MATERIALIZED VIEW trip_stats AS
    SELECT puz.zone as pickup_zone, doz.zone as dropoff_zone, count(td.vendorid) as trips, avg(td.tpep_dropoff_datetime - td.tpep_pickup_datetime) as avg_trip_time, min(td.tpep_dropoff_datetime - td.tpep_pickup_datetime) as min_trip_time, max(td.tpep_dropoff_datetime - td.tpep_pickup_datetime) as max_trip_time
    FROM trip_data td
    JOIN taxi_zone puz on td.pulocationid = puz.location_id
    JOIN taxi_zone doz on td.dolocationid = doz.location_id
    WHERE puz.zone <> doz.zone
    GROUP BY 1,2;


SELECT pickup_zone, dropoff_zone, avg_trip_time
FROM trip_stats
where avg_trip_time = (select max(avg_trip_time) from trip_stats);