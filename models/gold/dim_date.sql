{{ config(
    materialized='table'
) }}

-- Step 1: generate a complete, gap-free daily date spine
-- covering 2024-04-01 through 2024-09-27 (dbt_utils.date_spine's end_date
-- is exclusive, so 2024-09-28 is passed in to include 2024-09-27)
with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-04-01' as date)",
        end_date="cast('2024-09-28' as date)"
    ) }}

),

-- Step 2: derive every calendar attribute off each spine date
date_attributes as (

    select

        -- YYYYMMDD numeric key, e.g. 2024-04-01 -> 20240401
        to_number(to_char(date_day, 'YYYYMMDD')) as date_key,

        date_day as full_date,

        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,   -- numeric month: 1-12
        week(date_day) as week,

        -- Snowflake's numbering depends on session settings;
        -- day_name below gives a readable alternative
        dayofweek(date_day) as day_of_week,
        dayname(date_day) as day_name,

        -- US holidays falling inside this project's 2024 date window:
        -- Memorial Day 05-27, Juneteenth 06-19, Independence Day 07-04,
        -- Labor Day 09-02
        date_day in (
            date '2024-05-27',
            date '2024-06-19',
            date '2024-07-04',
            date '2024-09-02'
        ) as holiday_flag,

        -- Northern Hemisphere seasons
        case
            when month(date_day) in (12, 1, 2) then 'Winter'
            when month(date_day) in (3, 4, 5) then 'Spring'
            when month(date_day) in (6, 7, 8) then 'Summer'
            when month(date_day) in (9, 10, 11) then 'Fall'
        end as season

    from date_spine

)

-- Step 3: final column selection for the date dimension
select
    date_key,
    full_date,
    year,
    quarter,
    month,
    week,
    day_of_week,
    day_name,
    holiday_flag,
    season
from date_attributes