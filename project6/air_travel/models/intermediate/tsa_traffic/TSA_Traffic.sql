{{ config(
    post_hook=["drop table {{ ref('tmp_tsa_traffic_all') }}", "drop table {{ ref('tmp_tsa_traffic_duplicates') }}"] 
) }}

with int_TSA_Traffic as (
	select * 
	from {{ ref('tmp_tsa_traffic_all') }}
	except distinct
	select * 
	from {{ ref('tmp_tsa_traffic_duplicates') }}
)

select *
from int_TSA_Traffic
