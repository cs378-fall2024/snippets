with mrt_business_category_popularity_by_airport as (
	select a.name as airport, b.category, count(*) as count
	from {{ ref('Menu_Items') }} m join {{ ref('Business') }} b
	on m.business_name = b.name
	join {{ ref('Airport_Businesses') }} ab on b.name = ab.business
	join {{ ref('Airport') }} a on ab.icao = a.icao
	group by a.name, b.category
	order by a.name, count(*) desc
)

select *
from mrt_business_category_popularity_by_airport
