#!/bin/sh

# creates the dbt snapshot and model tables in the right sequence, 
# assumes that the raw tables have already been loaded

# tsa_traffic
dbt snapshot --select raw_tsa_traffic_snp
dbt run --select stg_tsa_traffic
dbt snapshot --select stg_tsa_traffic_snp

# airports
dbt snapshot --select raw_airports_snp
dbt run --select stg_airports
dbt snapshot --select stg_airports_snp
dbt run --select models/intermediate/airports/*
dbt snapshot --select int_Airport_snp

# airport_reviews
dbt snapshot --select raw_airport_reviews_snp
dbt run --select stg_airport_reviews
dbt snapshot --select stg_airport_reviews_snp
dbt run --select models/intermediate/airport_reviews/*
dbt run --select int_Airport_Review
dbt snapshot --select int_Airport_Review_snp

# marts
dbt run --select models/mart/*
dbt snapshot --select snapshots/mart/*
