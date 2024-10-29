{{ config(
    post_hook=["alter table {{ this }} add column dining BOOLEAN", 
	           "update {{ this }} set dining = True where menu_items is not null",
	           "update {{ this }} set dining = False where menu_items is null",
		       "delete from {{ this }} where name in (select name from {{ this }} group by name having count(*) > 1) and dining = false"])
}} 

with int_Business as (
	select * except(rank)
	from {{ ref('tmp_business_ranked') }}
	where rank = 1
)

select *
from int_Business
