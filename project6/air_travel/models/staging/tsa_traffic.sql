with stg_tsa_traffic as (
	select safe_cast(date as DATE format 'MM/DD/YYYY') as event_date,
	    safe_cast(hour as INTEGER) as event_hour,
	    airport_code,
	    airport_name,
	    city as airport_city,
	    state as airport_state,
	    checkpoint as tsa_checkpoint,
	    total_count as passenger_count,
	    _data_source,
	    _load_time
	from {{ source('air_travel_raw', 'tsa_traffic') }}
)

select *
from stg_tsa_traffic

