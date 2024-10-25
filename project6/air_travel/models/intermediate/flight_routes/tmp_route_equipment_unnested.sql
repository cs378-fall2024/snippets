with int_tmp_route_equipment_unnested as (
    select route_id, equipment
    from {{ ref('tmp_route_equipment') }} cross join unnest(equipment_array) as equipment
    where equipment != ''
)

select *
from int_tmp_route_equipment_unnested

