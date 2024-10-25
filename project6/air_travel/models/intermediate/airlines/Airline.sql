{{ config(
    post_hook=["drop table {{ ref('tmp_airline_countries_unmatched') }}", 
			   "drop table {{ ref('tmp_airline_countries_mapped') }}",
			   "drop table {{ ref('tmp_airline_countries_matched') }}",
			   "update {{ this }} set country = 'Canada' where country = 'Canadian Territories'",
			   "update {{ this }} set country = 'United States' where country = 'ALASKA'",
			   "update {{ this }} set country = 'Colombia' where country = 'AVIANCA'",
			   "update {{ this }} set country = 'Hong Kong'where country = 'DRAGON'",
			   "alter table {{ this }} add primary key (id) not enforced"]
) }}

with int_tmp_Airline as (
    select * except (_data_source, _load_time)
    from {{ ref('airlines') }}
    where country in (select name from {{ ref('Country') }})
    union distinct
    select *
    from {{ ref('tmp_airline_countries_matched' )}}
    where country is not null
	union distinct
    select * except (_data_source, _load_time)
	from {{ ref('airlines') }}
	where country in ('Canadian Territories', 'ALASKA', 'AVIANCA', 'DRAGON')
)

select *
from int_tmp_Airline
