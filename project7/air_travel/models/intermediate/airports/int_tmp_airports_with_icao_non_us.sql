{{
    config(
        unique_key='icao'
    )
}}

with int_tmp_airports_with_icao_non_us as (
	  select distinct icao, iata, name, city, string(null) as state, country,
	      latitude, longitude, altitude, timezone_name, timezone_delta, daylight_savings_time,
	      type, source, _load_time
	  from {{ ref("stg_airports_snp") }} 
	  where dbt_valid_to is null
	  and country != 'United States'
	  and type in ('airport', NULL)
	  and icao is not null
)

select *
from int_tmp_airports_with_icao_non_us
