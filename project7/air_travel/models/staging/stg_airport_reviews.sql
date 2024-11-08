{{
    config(
        unique_key='id'
    )
}}

with stg_airport_reviews as (
	select id,
	  threadRef as thread_id,
	  airportRef as airport_id,
	  airportIdent as airport_code,
	  date as date_created,
	  memberNickname as author,
	  subject,
	  body,
	  _data_source,
	  _load_time
	from {{ ref('raw_airport_reviews_snp') }}
	where dbt_valid_to is null
)

select *
from stg_airport_reviews

