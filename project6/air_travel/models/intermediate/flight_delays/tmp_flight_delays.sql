with int_tmp_flight_delays as (
    select distinct fd.event_month, fd.carrier, fd.carrier_name, ap.icao as airport_icao,
        fd.arr_total, fd.arr_cancelled, fd.arr_diverted, fd.arr_delay_min,
        fd.weather_delay_min, fd.nas_delay_min, fd.late_aircraft_delay_min,
    from {{ ref('flight_delays') }} fd join {{ ref('Airport') }} ap
    on fd.airport_code = ap.iata
    where ap.country = 'United States'
)

select *
from int_tmp_flight_delays
