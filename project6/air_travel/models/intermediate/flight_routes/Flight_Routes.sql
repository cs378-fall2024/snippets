/* {{ config(
    post_hook=["drop table {{ ref('tmp_flight_routes') }}", 
			   "drop table {{ ref('tmp_flight_routes_with_id') }}"]
) }} */

with int_Flight_Routes as (
    select route_id, airline_id, source_airport_icao, dest_airport_icao, codeshare, stops
    from {{ ref('tmp_flight_routes_with_id') }}
)

select *
from int_Flight_Routes
