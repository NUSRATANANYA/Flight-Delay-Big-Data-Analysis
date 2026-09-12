-- Flight Delay Dataset 2024
-- Pig Analysis 1: Total Flights by Airline

flights = LOAD '/flight_delay_project/input/flight_data_2024.csv'
USING PigStorage(',')
AS (
    year:chararray,
    month:chararray,
    day_of_month:chararray,
    day_of_week:chararray,
    fl_date:chararray,
    op_unique_carrier:chararray,
    op_carrier_fl_num:chararray,
    origin:chararray,
    origin_city_name:chararray,
    origin_state_nm:chararray,
    dest:chararray,
    dest_city_name:chararray,
    dest_state_nm:chararray,
    crs_dep_time:chararray,
    dep_time:chararray,
    dep_delay:chararray,
    taxi_out:chararray,
    wheels_off:chararray,
    wheels_on:chararray,
    taxi_in:chararray,
    crs_arr_time:chararray,
    arr_time:chararray,
    arr_delay:chararray,
    cancelled:chararray,
    cancellation_code:chararray,
    diverted:chararray,
    crs_elapsed_time:chararray,
    actual_elapsed_time:chararray,
    air_time:chararray,
    distance:chararray,
    carrier_delay:chararray,
    weather_delay:chararray,
    nas_delay:chararray,
    security_delay:chararray,
    late_aircraft_delay:chararray
);

-- Remove CSV header
clean_flights = FILTER flights
    BY op_unique_carrier != 'op_unique_carrier'
    AND op_unique_carrier IS NOT NULL;

-- Group records by airline
grouped_airlines = GROUP clean_flights BY op_unique_carrier;

-- Count flights
airline_counts = FOREACH grouped_airlines GENERATE
    group AS airline,
    COUNT(clean_flights) AS total_flights;

-- Sort highest to lowest
sorted_counts = ORDER airline_counts BY total_flights DESC;

DUMP sorted_counts;