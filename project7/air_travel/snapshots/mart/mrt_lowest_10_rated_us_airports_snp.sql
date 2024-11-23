{% snapshot mrt_lowest_10_rated_us_airports_snp %}

    {{
        config(
          target_schema='inc_air_travel_snp',
          strategy='check',
          unique_key='id',
          check_cols=['num_reviews'],
        )
    }}

    select name || '-' || icao || '-' || city || '-' || state || '-' || sentiment as id, * 
	from {{ ref('mrt_lowest_10_rated_us_airports') }}

{% endsnapshot %}