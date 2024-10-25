with mrt_highest_rated_airports_us as (
	select a.name as airport_name, a.city, a.state, ar.sentiment, count(*) as num_reviews
	from {{ ref('Airport') }} a join {{ ref('Airport_Review') }} ar
	on a.icao = ar.icao
	where ar.sentiment = 'positive'
	and ar.relevance = true
	and a.country = 'United States'
	group by a.name, a.city, a.state, ar.sentiment
	order by count(*) desc	
)

select *
from mrt_highest_rated_airports_us
