{% snapshot stg_airports_snp %}

    {{
        config(
          target_schema='inc_air_travel_snp',
          strategy='timestamp',
          unique_key='airport_id',
          updated_at='_load_time',
        )
    }}

    select * 
	from {{ ref('stg_airports') }}

{% endsnapshot %}
