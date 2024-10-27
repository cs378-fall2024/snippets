with int_Flight_Delays as (
	 select fd.event_month, al.id as airline_id, fd.airport_icao, fd.arr_total, 
	 	fd.arr_cancelled, fd.arr_diverted, fd.arr_delay_min, fd.weather_delay_min, 
		fd.nas_delay_min, fd.late_aircraft_delay_min
	 from {{ ref('tmp_flight_delays') }} fd join {{ ref('Airline') }} al
	 on fd.carrier = al.iata
	 where al.country = 'United States'
)

select *
from int_Flight_Delays
