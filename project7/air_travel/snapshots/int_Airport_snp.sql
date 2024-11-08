{% snapshot int_Airport_snp %}

    {{
        config(
          target_schema='inc_air_travel_snp',
          strategy='timestamp',
          unique_key='icao',
          updated_at='_load_time',
        )
    }}

    select * 
	from {{ ref('int_Airport') }}

{% endsnapshot %}
