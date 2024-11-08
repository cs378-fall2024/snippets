{{
    config(
        unique_key='icao'
    )
}}

with mrt_top_10_rated_us_airports as (
	select a.name, a.icao, a.city, a.state, ar.sentiment, count(*) as num_reviews
	from {{ ref('int_Airport_snp') }} a join {{ ref('int_Airport_Review_snp') }} ar
	on a.icao = ar.icao
	where ar.sentiment = 'positive'
	and ar.relevance = true
	and a.country = 'United States'
	and (a.dbt_valid_to is null or ar.dbt_valid_to is null)
	group by a.name, a.icao, a.city, a.state, ar.sentiment
	order by count(*) desc	
)

select *
from mrt_top_10_rated_us_airports
