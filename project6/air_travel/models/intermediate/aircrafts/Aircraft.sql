{{ config(
    post_hook=["delete from {{ this }} where icao = 'B737' and iata = '737'",
		 	   "delete from {{ this }} where icao = 'E135' and iata = 'ERD'",
			   "delete from {{ this }} where icao = 'A338' and name = 'Airbus A330-700 Beluga XL'",
			   "delete from {{ this }} where icao = 'B737' and iata = '73G' and name = 'Boeing 737'"] 
) }}

with int_Aircraft as (
	select t.icao, t.iata, a.name
	from {{ ref('tmp_aircrafts') }} t join {{ ref('aircrafts' )}} a 
	on a.name = t.name
	where a.icao is null or a.iata is null
	union distinct
	select icao, iata, name
	from {{ ref('aircrafts' )}}
	where icao is not null and iata is not null
)

select *
from int_Aircraft
where icao is not null
