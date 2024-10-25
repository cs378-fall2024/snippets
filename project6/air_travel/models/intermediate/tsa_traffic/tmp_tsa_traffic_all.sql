with int_tmp_tsa_traffic_all as (
	select distinct t.event_date, t.event_hour, a.icao as airport_icao, t.tsa_checkpoint, t.passenger_count
	from {{ ref('Airport') }} a join {{ ref('tsa_traffic') }} t
	on a.iata = t.airport_code
	where a.country = 'United States'
)

select *
from int_tmp_tsa_traffic_all
