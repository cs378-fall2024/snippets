#!/bin/sh

# updates the dbt snapshot and model tables in the right sequence, 
# assumes that the raw tables have already been loaded and are ready 

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
