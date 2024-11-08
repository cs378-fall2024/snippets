{{
    config(
        unique_key='id'
    )
}}

with stg_tsa_traffic as (
		select id, safe_cast(date as DATE format 'MM/DD/YYYY') as event_date,
		    safe_cast(hour as INTEGER) as event_hour,
		    airport_code,
		    airport_name,
		    city as airport_city,
		    state as airport_state,
		    checkpoint as tsa_checkpoint,
		    total_count as passenger_count,
		    _data_source,
		    _load_time
		from {{ ref('raw_tsa_traffic_snp') }}
		where dbt_valid_to is null
	except distinct
		select id, safe_cast(date as DATE format 'MM/DD/YYYY') as event_date,
		    safe_cast(hour as INTEGER) as event_hour,
		    airport_code,
		    airport_name,
		    city as airport_city,
		    state as airport_state,
		    checkpoint as tsa_checkpoint,
		    total_count as passenger_count,
		    _data_source,
		    _load_time
		from {{ ref('raw_tsa_traffic_snp') }}
		where dbt_valid_to is null
		and ((airport_code = 'AMA' and state = 'WA')
		or (airport_code = 'ANC' and state = 'TX')
		or (airport_code = 'ATL' and state = 'AK') 
		or (airport_code = 'BRD' and state = 'TX') 
		or (airport_code = 'CNY' and state = 'IL')
		or (airport_code = 'CPR' and state = 'TX')
		or (airport_code = 'CRW' and state = 'WW')
	    or (airport_code = 'FAT' and state = 'AK')
		or (airport_code = 'GRB' and state = 'CO')
		or (airport_code = 'HTS' and state = 'WW')
		or (airport_code = 'KCM')
		or (airport_code = 'LBE' and state = 'NH')
		or (airport_code = 'LWB' and state = 'WW')
		or (airport_code = 'PKB' and state = 'WW')
		or (airport_code = 'SGU' and state = 'LA')
		or (airport_code = 'SHD' and state = 'LA')
		or (airport_code = 'SHV' and state = 'VA'))	
)

select *
from stg_tsa_traffic