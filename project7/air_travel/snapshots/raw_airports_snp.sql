{% snapshot raw_airports_snp %}

    {{
        config(
          target_schema='inc_air_travel_snp',
          strategy='timestamp',
          unique_key='airport_id',
          updated_at='_load_time',
        )
    }}

    select * 
	from {{ source('air_travel_raw', 'airports') }}

{% endsnapshot %}
