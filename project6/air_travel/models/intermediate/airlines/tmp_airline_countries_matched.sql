with int_tmp_airline_countries_matched as (
    select a.id, a.name, a.alias, a.iata, a.icao, a.callsign, m.new as country, a.active 
    from {{ ref('tmp_airline_countries_mapped') }} m join {{ ref('airlines') }} a
    on m.current = a.country 
	where a.country in (select country from {{ ref('tmp_airline_countries_unmatched') }}) 
)

select * 
from int_tmp_airline_countries_matched
    
