{% snapshot raw_airport_reviews_snp %}

    {{
        config(
          target_schema='inc_air_travel_snp',
          strategy='timestamp',
          unique_key='id',
          updated_at='_load_time',
        )
    }}

    select * 
	from {{ source('air_travel_raw', 'airport_reviews') }}

{% endsnapshot %}
