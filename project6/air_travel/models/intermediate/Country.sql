with int_Country as (
	select name, iso_code, array_agg(ifnull(dafif_code, 'Unknown')) as dafif_codes
	from {{ ref('countries') }}
	group by name, iso_code
)

select *
from int_Country
