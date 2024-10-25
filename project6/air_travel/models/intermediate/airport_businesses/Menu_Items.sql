{{ config(
    post_hook="alter table {{ ref('Business') }} drop column if exists menu_items" 
) }}

with int_Menu_Items as (
	select name as business_name, split(menu_items, ',') as menu_items_array
	from {{ ref('Business') }}
	where dining = True
)

select business_name, menu_item
from int_Menu_Items, unnest(menu_items_array) as menu_item


