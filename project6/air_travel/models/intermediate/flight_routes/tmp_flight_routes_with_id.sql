with int_tmp_flight_routes_with_id as (
	select row_number() over () as route_id,
	   airline_id, source_airport_icao, dest_airport_icao, codeshare, stops, equipment
	from {{ ref('tmp_flight_routes') }}
)

select *
from int_tmp_flight_routes_with_id

