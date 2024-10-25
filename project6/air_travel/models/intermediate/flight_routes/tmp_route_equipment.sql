with int_tmp_route_equipment as (
	  select route_id, split(equipment, ' ') as equipment_array
	  from {{ ref('tmp_flight_routes_with_id') }}
	  where equipment is not null
)

select *
from int_tmp_route_equipment

