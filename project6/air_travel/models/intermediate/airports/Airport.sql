{{ config(
    post_hook=["delete from {{ this }} where icao is null",
			   "delete from {{ this }} where source = 'User' and icao in 
			   		(select icao from {{ this }} group by icao having count(*) > 1)",
			   "insert into {{ ref('Country') }} (name) select distinct country from {{ this }} 
				  where country not in (select name from {{ ref('Country') }})"] 
) }} 

with int_Airport as (
	select icao, iata, name, city, state, country, latitude, longitude, altitude, 
		timezone_name, timezone_delta, daylight_savings_time, type, source
	from {{ ref("tmp_airports_with_icao_us") }}
	union distinct
	select icao, iata, name, city, state, country, latitude, longitude, altitude, 
		timezone_name, timezone_delta, daylight_savings_time, type, source
	from {{ ref("tmp_airports_merged_icao_us") }}
	union distinct
	select icao, iata, name, city, state, country, latitude, longitude, altitude, 
		timezone_name, timezone_delta, daylight_savings_time, type, source
	from {{ ref("tmp_airports_with_icao_non_us") }}
	union distinct
	select icao, iata, name, city, state, country, latitude, longitude, altitude, 
		timezone_name, timezone_delta, daylight_savings_time, type, source
	from {{ ref("tmp_airports_merged_icao_non_us") }}
)

select *
from int_Airport

