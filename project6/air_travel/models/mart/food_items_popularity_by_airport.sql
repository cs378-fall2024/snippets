with mrt_food_items_popularity_by_airport as (
	select a.name as airport, menu_item, count(*) as count
	from {{ ref('Menu_Items') }} m join {{ ref('Business') }} b
	on m.business_name = b.name
	join {{ ref('Airport_Businesses') }} ab on b.name = ab.business
	join {{ ref('Airport') }} a on ab.icao = a.icao
	group by a.name, menu_item
	order by a.name, count(*) desc
)

select *
from mrt_food_items_popularity_by_airport
