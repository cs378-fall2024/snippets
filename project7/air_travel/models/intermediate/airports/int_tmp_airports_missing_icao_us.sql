{{
    config(
        unique_key='icao'
    )
}}

with int_tmp_airports_missing_icao_us as (
	select icao, iata, name, city, country, latitude, longitude, altitude, timezone_name,
	        timezone_delta, daylight_savings_time, type, source, _load_time
	from {{ ref("stg_airports_snp") }}
	where dbt_valid_to is null
	and country = 'United States'
	and type in ('airport', null)
	and icao is null
)

select *
from int_tmp_airports_missing_icao_us
