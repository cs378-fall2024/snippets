{{
    config(
        unique_key='icao'
    )
}}

{{ config(
    post_hook=["delete from {{ this }} where icao is null",
			   "delete from {{ this }} where source = 'User' and icao in 
			   		(select icao from {{ this }} group by icao having count(*) > 1)"] 
) }} 

with int_Airport as (
	select icao, iata, name, city, state, country, latitude, longitude, altitude, 
		timezone_name, timezone_delta, daylight_savings_time, type, source, _load_time
	from {{ ref("int_tmp_airports_with_icao_us") }}
	
	{% if is_incremental() %}
		where _load_time > (select max(_load_time) from {{ this }})
	{% endif %}
	
	union distinct
	select icao, iata, name, city, state, country, latitude, longitude, altitude, 
		timezone_name, timezone_delta, daylight_savings_time, type, source, _load_time
	from {{ ref("int_tmp_airports_merged_icao_us") }}
	
	{% if is_incremental() %}
		where _load_time > (select max(_load_time) from {{ this }})
	{% endif %}
	
	union distinct
	select icao, iata, name, city, state, country, latitude, longitude, altitude, 
		timezone_name, timezone_delta, daylight_savings_time, type, source, _load_time
	from {{ ref("int_tmp_airports_with_icao_non_us") }}
	
	{% if is_incremental() %}
		where _load_time > (select max(_load_time) from {{ this }})
	{% endif %}
	
	union distinct
	select icao, iata, name, city, state, country, latitude, longitude, altitude, 
		timezone_name, timezone_delta, daylight_savings_time, type, source, _load_time
	from {{ ref("int_tmp_airports_merged_icao_non_us") }}
	
	{% if is_incremental() %}
		where _load_time > (select max(_load_time) from {{ this }})
	{% endif %}
)

select *
from int_Airport

