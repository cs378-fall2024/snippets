{{
    config(
        unique_key='icao'
    )
}}

with int_tmp_airports_merged_icao_us as (
	select f.icao, f.iata, f.name, m.city, f.state, m.country, m.latitude, m.longitude, m.altitude, 
		m.timezone_name, m.timezone_delta, m.daylight_savings_time, m.type, m.source, m._load_time
	from {{ ref('int_tmp_airports_filled_out_icao_us') }} f join {{ ref('int_tmp_airports_missing_icao_us') }} m
	on f.name = m.name
	{% if is_incremental() %}
		where _load_time > (select max(_load_time) from {{ this }})
	{% endif %}
)

select *
from int_tmp_airports_merged_icao_us