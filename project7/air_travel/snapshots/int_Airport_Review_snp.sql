{% snapshot int_Airport_Review_snp %}

    {{
        config(
          target_schema='inc_air_travel_snp',
          strategy='timestamp',
          unique_key='id',
          updated_at='_load_time',
        )
    }}

    select * 
	from {{ ref('int_Airport_Review') }}

{% endsnapshot %}
