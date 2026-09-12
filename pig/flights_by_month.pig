-- Pig Analysis 2: Total Flights by Month

flights = LOAD '/flight_delay_project/input/flight_data_2024.csv'
USING PigStorage(',')
AS (
    year:chararray,
    month:chararray,
    day_of_month:chararray,
    day_of_week:chararray,
    fl_date:chararray,
    op_unique_carrier:chararray
);

-- Remove header
clean_flights = FILTER flights
    BY year != 'year'
    AND month IS NOT NULL;

-- Group by month
monthly_group = GROUP clean_flights BY month;

-- Count flights for each month
monthly_count = FOREACH monthly_group GENERATE
    group AS month,
    COUNT(clean_flights) AS total_flights;

-- Sort by month
monthly_sorted = ORDER monthly_count BY month ASC;

DUMP monthly_sorted;