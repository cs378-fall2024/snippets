with mrt_airport_travelers_by_month_year as (
	select business, airport, city, state, terminal,
	    case month_of_year
	      when 1 then 'January'
	      when 2 then 'February'
	      when 3 then 'March'
	      when 4 then 'April'
	      when 5 then 'May'
	      when 6 then 'June'
	      when 7 then 'July'
	      when 8 then 'August'
	      when 9 then 'September'
	      when 10 then 'October'
	      when 11 then 'November'
	      when 12 then 'Decemver'
	    end as month_of_year, num_travelers
	  from
	  (
	    select b.name as business, a.name as airport, a.city, a.state, ab.terminal,
	      extract(month from t.event_date) as month_of_year,
	      sum(t.passenger_count) as num_travelers
	    from {{ ref('Airport_Businesses') }} ab join {{ ref('Business') }} b
	    on ab.business = b.name
	    join {{ ref('Airport') }} a on ab.icao = a.icao
	    join {{ ref('TSA_Traffic') }} t on a.icao = t.airport_icao
	    group by b.name, a.name, a.city, a.state, ab.terminal, extract(month from t.event_date)
	    order by month_of_year, num_travelers desc
	  )
)

select *
from mrt_airport_travelers_by_month_year
