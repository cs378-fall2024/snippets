with int_tmp_airports_missing_icao_non_us as (
	select icao, iata, name, city, string(null) as state, country, latitude, longitude, altitude, timezone_name,
	        timezone_delta, daylight_savings_time, type, source
	from {{ ref("airports") }}
	where country != 'United States'
	and type in ('airport', null)
	and icao is null
)

select *
from int_tmp_airports_missing_icao_non_us
