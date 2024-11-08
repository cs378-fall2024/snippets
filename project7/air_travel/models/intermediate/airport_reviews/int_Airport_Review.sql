{{
    config(
        unique_key='id'
    )
}}

with int_Airport_Review as (
	select r.id, r.thread_id, r.icao, r.date_created, r.author, r.subject, r.body, s.relevance, s.sentiment, r._load_time
	from {{ ref('int_tmp_airport_reviews') }} r
	left join {{ ref('int_tmp_airport_reviews_with_sentiment') }} s
	on r.id = s.id
	where r.icao in (select distinct icao from {{ ref('int_Airport_snp') }}  where dbt_valid_to is null)
	
	-- use the is_incremental() macro to detect if this is the first run (i.e. we're
	-- creating the table) or if this is a subsequent run (i.e. we're upserting into the table) 
    {% if is_incremental() %}
	   and r._load_time > (select max(_load_time) from {{ this }})
    {% endif %}
)

select *
from int_Airport_Review
where id not in (select id from int_Airport_Review group by id having count(*) > 1)
