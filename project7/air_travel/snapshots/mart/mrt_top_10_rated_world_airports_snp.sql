{% snapshot mrt_top_10_rated_world_airports_snp %}

    {{
        config(
          target_schema='inc_air_travel_snp',
          strategy='check',
          unique_key='id',
          check_cols=['num_reviews'],
        )
    }}

    select name || '-' || icao || '-' || city || '-' || country || '-' || sentiment as id, *  
	from {{ ref('mrt_top_10_rated_world_airports') }}

{% endsnapshot %}