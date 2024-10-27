with int_Route_Equipment as (
    select re.route_id, a.icao as aircraft_icao
    from {{ ref('tmp_route_equipment_unnested') }} re join {{ ref('Aircraft') }} a
    on re.equipment = a.iata
)

select *
from int_Route_Equipment





