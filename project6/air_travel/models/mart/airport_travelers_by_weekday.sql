with mrt_airport_travelers_by_weekday as (
	select business, airport, city, state, terminal,
	  case weekday
	    when 1 then 'Sunday'
	    when 2 then 'Monday'
	    when 3 then 'Tuesday'
	    when 4 then 'Wednesday'
	    when 5 then 'Thursday'
	    when 6 then 'Friday'
	    when 7 then 'Saturday'
	  end as weekday, num_travelers
	  from
	    (select b.name as business, a.name as airport, a.city, a.state, ab.terminal, extract(dayofweek from t.event_date) as weekday,
	    round(avg(t.passenger_count), 2) as num_travelers
	    from {{ ref('Airport_Businesses') }} ab join {{ ref('Business') }} b
	    on ab.business = b.name
	    join {{ ref('Airport') }} a
	    on ab.icao = a.icao
	    join {{ ref('TSA_Traffic') }} t
	    on a.icao = t.airport_icao
	    group by b.name, a.name, a.city, a.state, ab.terminal, extract(dayofweek from t.event_date)
	    order by weekday, num_travelers desc)
	)

select *
from mrt_airport_travelers_by_weekday
