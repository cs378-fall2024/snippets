with int_tmp_unmatched_airline_countries as (
	select distinct country 
	from {{ ref('airlines') }}
	where country not in (select name from {{ ref('Country') }})
)

select *
from int_tmp_unmatched_airline_countries
