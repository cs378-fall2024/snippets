with int_Flight_Routes as (
    select route_id, airline_id, source_airport_icao, dest_airport_icao, codeshare, stops
    from {{ ref('tmp_flight_routes_with_id') }}
	where source_airport_icao in (select icao from {{ ref('Airport') }})
	and dest_airport_icao in (select icao from {{ ref('Airport') }})
)

select *
from int_Flight_Routes

