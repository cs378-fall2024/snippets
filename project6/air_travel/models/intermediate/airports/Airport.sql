/* {{ config(
    post_hook=["drop table {{ ref('tmp_airports_with_icao_us') }}", 
			   "drop table {{ ref('tmp_airports_merged_icao_us') }}", 
			   "drop table {{ ref('tmp_airports_with_icao_non_us') }}", 
			   "drop table {{ ref('tmp_airports_merged_icao_non_us') }}",
			   "alter table {{ this }} add primary key (icao) not enforced"] 
) }} */

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
