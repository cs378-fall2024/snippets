{{ config(
    post_hook="drop table {{ ref('tmp_business') }}" 
) }}

with int_sorted_name_categories as (
	select name, category, count(*) as num_businesses
	from {{ ref('tmp_business') }}
	group by name, category	    
),
int_sorted_categories as
    (select category, count(*) as num_businesses
    from {{ ref('tmp_business') }}
    group by category)

select row_number() over (partition by name order by 
	(select num_businesses 
	from int_sorted_name_categories s 
	where b.category = s.category and b.name = s.name) desc, 
	length(menu_items) desc) as rank, *
from {{ ref('tmp_business') }} b
where menu_items is not null
union distinct
select row_number() over (partition by name order by 
	(select num_businesses from int_sorted_categories s where b. category = s.category) desc) AS rank, *
from {{ ref('tmp_business') }} b
where menu_items is null
