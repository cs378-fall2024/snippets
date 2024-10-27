{{ config(
    post_hook="insert into {{ ref('Country') }} (name) select distinct country from {{ this }} 
				where country not in (select name from {{ ref('Country') }})"
) }}

with int_tmp_Airline as (
    select id, name, alias, iata, icao, callsign, country, active
    from {{ ref('airlines') }}
    where country in (select name from {{ ref('Country') }})
    union distinct
    select id, name, alias, iata, icao, callsign, country, active
    from {{ ref('tmp_airline_countries_matched' )}}
    where country is not null
	union distinct
    select id, name, alias, iata, icao, callsign, 
	  case country
	    when 'Canadian Territories' then 'Canada'
	    when 'ALASKA' then 'United States'
	    when 'AVIANCA' then 'Colombia'
		when 'DRAGON' then 'Hong Kong'
	  end as country, 
	  active
	from {{ ref('airlines') }}
	where country in ('Canadian Territories', 'ALASKA', 'AVIANCA', 'DRAGON')
)

select distinct *
from int_tmp_Airline
