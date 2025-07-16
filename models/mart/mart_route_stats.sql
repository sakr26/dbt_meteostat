SELECT origin, a1.name AS origin_name, a1.city AS origin_city, a1.country AS origin_country, dest, a2.name AS dest_name, a2.city AS dest_city, a2.country AS dest_country, count(origin) AS count_total_flights_or, count(dest) AS count_total_flights_dest, count(DISTINCT tail_number) AS count_unique_tailnumber, count(DISTINCT airline) AS count_unique_airline, avg(actual_elapsed_time) AS avg_elapsed_time, avg(arr_delay) AS avg_arr_delay, min(arr_delay) AS min_arr_delay, max(arr_delay) AS max_arr_delay, sum(cancelled) AS number_of_cancelled_flights, sum(diverted) AS number_of_diverted_flights
FROM {{ref('prep_flights')}}
JOIN {{ref('staging_airports')}} AS a1
ON origin = a1.faa
JOIN {{ref('staging_airports')}} AS a2
ON dest = a2.faa
GROUP BY origin, dest, a1.name, a2.name, a1.city, a2.city, a1.country, a2.country
ORDER BY origin