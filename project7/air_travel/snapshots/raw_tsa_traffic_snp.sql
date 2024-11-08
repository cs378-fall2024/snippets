{% snapshot raw_tsa_traffic_snp %}

    {{
        config(
          target_schema='inc_air_travel_snp',
          strategy='timestamp',
          unique_key='id',
          updated_at='_load_time',
        )
    }}

    select date || '-' || hour || '-' || airport_name || '-' || airport_code || '-' || checkpoint || '-' || total_count as id, * 
	from {{ source('air_travel_raw', 'tsa_traffic') }}

{% endsnapshot %}
