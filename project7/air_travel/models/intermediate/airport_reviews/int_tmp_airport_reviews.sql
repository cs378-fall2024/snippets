{{
    config(
        unique_key='id'
    )
}}

with int_tmp_airport_reviews as (
	 select distinct id, thread_id, airport_code as icao, date_created, author,
	    subject, body, _load_time
	 from {{ ref('stg_airport_reviews_snp') }}
	 where dbt_valid_to is null
	 and airport_code in (select icao from {{ ref('stg_airports_snp') }} where dbt_valid_to is null)
)

select *
from int_tmp_airport_reviews
