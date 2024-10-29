with int_TSA_Traffic as (
	select * 
	from {{ ref('tmp_tsa_traffic_all') }}
	except distinct
	select * 
	from {{ ref('tmp_tsa_traffic_duplicates') }}
)

select *
from int_TSA_Traffic
where event_date is not null
and event_hour is not null
and airport_icao is not null
and tsa_checkpoint is not null
and passenger_count is not null 
