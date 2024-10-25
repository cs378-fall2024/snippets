with int_tmp_airports_with_icao_us as (
	  select distinct a.icao, a.iata, a.name, a.city, tsa.airport_state as state, a.country,
	      a.latitude, a.longitude, a.altitude, a.timezone_name, a.timezone_delta, a.daylight_savings_time,
	      a.type, a.source
	  from {{ ref("airports") }} a
	  left join {{ ref("tsa_traffic") }} tsa
	  on a.iata = tsa.airport_code
	  where a.country = 'United States'
	  and a.type in ('airport', NULL)
	  and a.icao is not null
)

select *
from int_tmp_airports_with_icao_us
