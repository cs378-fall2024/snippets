with int_tmp_tsa_traffic_duplicates as (
	select * 
	from {{ ref('tmp_tsa_traffic_all') }}
	where struct(event_date, event_hour, airport_icao, tsa_checkpoint) in
	      (select struct(event_date, event_hour, airport_icao, tsa_checkpoint)
	       from {{ ref('tmp_tsa_traffic_all') }}
	       group by event_date, event_hour, airport_icao, tsa_checkpoint
	       having count(*) > 1)
)

select *
from int_tmp_tsa_traffic_duplicates
