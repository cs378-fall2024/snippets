{{
    config(
        unique_key='icao'
    )
}}

with mrt_lowest_10_rated_world_airports as (
	select a.name, a.icao, a.city, a.country, ar.sentiment, count(*) as num_reviews
	from {{ ref('int_Airport_snp') }} a join {{ ref('int_Airport_Review_snp') }} ar
	on a.icao = ar.icao
	where ar.sentiment = 'negative'
	and ar.relevance = true
	and a.country != 'United States'
	and (a.dbt_valid_to is null or ar.dbt_valid_to is null)
	group by a.name, a.icao, a.city, a.country, ar.sentiment
	order by count(*) desc	
)

select *
from mrt_lowest_10_rated_world_airports
