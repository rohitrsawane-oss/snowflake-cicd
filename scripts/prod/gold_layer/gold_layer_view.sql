USE DATABASE {DATABASE_NAME};
--bronze layer tables
USE ROLE FR_PROD_DATA_ENGINEER;

create or replace dynamic table TRUSTCAB_PROD.SCH_TRUSTCAB_GOLD.EXECUTIVE_VIEW(
	CITY_ID,
	CITY_NAME,
	MONTH,
	MONTH_NAME,
	START_OF_MONTH,
	TOTAL_TRIPS,
	TOTAL_REVENUE WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	TOTAL_DISTANCE_KM WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	AVG_PASSENGER_RATING,
	AVG_DRIVER_RATING,
	DISTANCE_KM_NEW,
	DISTANCE_KM_REPEATED,
	REVENUE_NEW WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	REVENUE_REPEATED WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	TRIPS_NEW,
	TRIPS_REPEATED,
	REVENUE_WEEKDAY WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	REVENUE_WEEKEND WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	TRIPS_WEEKDAY,
	TRIPS_WEEKEND,
	DAY_TYPE,
	NEW_PASSENGERS,
	REPEAT_PASSENGERS,
	TOTAL_PASSENGERS,
	TOTAL_TARGET_TRIPS,
	TARGET_NEW_PASSENGERS,
	AVG_FARE_PER_KM_NEW WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	AVG_FARE_PER_KM_REPEAT WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO')
) target_lag = '10 days' refresh_mode = AUTO initialize = ON_CREATE warehouse = WH_PROD_DATA_ENGINEER
 as
WITH base AS (
  SELECT
      ft.trip_id,
      ft.city_id,
      dc.city_name,
      dd.start_of_month,
      TO_VARCHAR(dd.start_of_month, 'YYYY-MM') AS month,
      dd.month_name,
      dd.day_type,
      LOWER(ft.passenger_type) 
                AS passenger_type,
      ft.fare_amount,
      ft.passenger_rating,
      ft.driver_rating,
      ft.DISTANCE_TRAVELLED_KM              AS distance_km
  FROM TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.FACT_TRIPS_DYNAMIC ft
  JOIN TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.DIM_DATE_DYNAMIC  dd ON ft.date    = dd.date
  JOIN TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.DIM_CITY_DYNAMIC  dc ON ft.city_id = dc.city_id
),
agg AS (
  SELECT
      city_id,
      city_name,
   
   month,
      month_name,
      start_of_month,
      day_type,

      COUNT(*)                               AS total_trips,
      SUM(fare_amount)                       AS total_revenue,
      SUM(distance_km)     
                  AS total_distance_km,
      AVG(passenger_rating)                  AS avg_passenger_rating,
      AVG(driver_rating)                     AS avg_driver_rating,

      /* splits by passenger type */
      SUM(CASE WHEN passenger_type ILIKE 'new%' THEN distance_km ELSE 0 END)  
   AS distance_km_new,
      SUM(CASE WHEN passenger_type ILIKE 'rep%' THEN distance_km ELSE 0 END)     AS distance_km_repeated,
      SUM(CASE WHEN passenger_type ILIKE 'new%' THEN fare_amount  ELSE 0 END)    AS revenue_new,
      SUM(CASE WHEN passenger_type ILIKE 'rep%' THEN fare_amount  ELSE 0 END)    AS revenue_repeated,
      SUM(CASE WHEN passenger_type ILIKE 'new%' THEN 1            ELSE 0 END)    AS trips_new,
  
    SUM(CASE WHEN passenger_type ILIKE 'rep%' THEN 1            ELSE 0 END)    AS trips_repeated,

      /* splits by day type */
      SUM(CASE WHEN day_type = 'Weekday' THEN fare_amount ELSE 0 END)            AS revenue_Weekday,
      SUM(CASE WHEN day_type = 'Weekend' THEN fare_amount ELSE 0 END)            AS revenue_Weekend,
    
  SUM(CASE WHEN day_type = 'Weekday' THEN 1           ELSE 0 END)            AS trips_Weekday,
      SUM(CASE WHEN day_type = 'Weekend' THEN 1           ELSE 0 END)            AS trips_Weekend
  FROM base
  GROUP BY 1,2,3,4,5,6
),
pax AS (
  SELECT
      city_id,
      TO_VARCHAR(month::date, 'YYYY-MM')    
       AS month,
      SUM(new_passengers)                    AS new_passengers,
      SUM(repeat_passenger)                 AS repeat_passengers,
      SUM(total_passengers)                  AS total_passengers
  FROM TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.FACT_PASSENGER_SUMMARY_DYNAMIC
  GROUP BY 1,2
),
tgt AS (
  SELECT
     
 COALESCE(t.city_id, n.city_id)                                          AS city_id,
      COALESCE(TO_VARCHAR(t.month, 'YYYY-MM'), TO_VARCHAR(n.month::date, 'YYYY-MM')) AS month,
      SUM(t.total_target_trips)                                      
         AS total_target_trips,
      SUM(n.target_new_passengers)                                            AS target_new_passengers
  FROM TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.MONTHLY_TARGET_TRIPS_DYNAMIC t
  FULL OUTER JOIN TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.MONTHLY_TARGET_NEW_PASSENGERS_DYNAMIC n
    ON t.city_id = n.city_id AND t.month = n.month
  GROUP BY 1,2
)
SELECT
    a.city_id,
    a.city_name,
    a.month,
  
  a.month_name,
    a.start_of_month,
    a.total_trips,
    a.total_revenue,
    a.total_distance_km,
    a.avg_passenger_rating,
    a.avg_driver_rating,
    a.distance_km_new,
    a.distance_km_repeated,
    a.revenue_new,
    a.revenue_repeated,
    a.trips_new,
    a.trips_repeated,
    a.revenue_Weekday,
    a.revenue_Weekend,
    a.trips_Weekday,
    a.trips_Weekend,
    a.day_type,
    p.new_passengers,
    p.repeat_passengers,
    p.total_passengers,
    t.total_target_trips,
    t.target_new_passengers,
    IFF(NULLIF(a.distance_km_new,      
0) IS NULL, NULL, a.revenue_new      / NULLIF(a.distance_km_new,      0)) AS avg_fare_per_km_new,
    IFF(NULLIF(a.distance_km_repeated, 0) IS NULL, NULL, a.revenue_repeated / NULLIF(a.distance_km_repeated, 0)) AS avg_fare_per_km_repeat
FROM agg a
LEFT JOIN pax p  ON p.city_id = a.city_id AND p.month = a.month
LEFT JOIN tgt t  ON t.city_id = a.city_id AND t.month = a.month
;


create or replace dynamic table TRUSTCAB_PROD.SCH_TRUSTCAB_GOLD.CITY_VIEW(
	CITY_ID,
	CITY_NAME,
	MONTH,
	MONTH_NAME,
	START_OF_MONTH,
	TOTAL_TRIPS,
	TOTAL_REVENUE WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	TOTAL_DISTANCE_KM,
	AVG_PASSENGER_RATING,
	AVG_DRIVER_RATING,
	DISTANCE_KM_NEW,
	DISTANCE_KM_REPEATED,
	REVENUE_NEW WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	REVENUE_REPEATED WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	TRIPS_NEW,
	TRIPS_REPEATED,
	REVENUE_WEEKDAY WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	REVENUE_WEEKEND WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	TRIPS_WEEKDAY,
	TRIPS_WEEKEND,
	DAY_TYPE,
	NEW_PASSENGERS,
	REPEAT_PASSENGERS,
	TOTAL_PASSENGERS,
	TOTAL_TARGET_TRIPS,
	TARGET_NEW_PASSENGERS,
	AVG_FARE_PER_KM_NEW WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	AVG_FARE_PER_KM_REPEAT WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	TRIPS_BUCKET,
	REPEAT_PASSENGER_COUNT,
	BUCKET_SHARE
) target_lag = '10 days' refresh_mode = AUTO initialize = ON_CREATE warehouse = WH_PROD_DATA_ENGINEER
 as
WITH rtd AS (
  SELECT
      city_id,
      TO_VARCHAR(month::DATE, 'YYYY-MM') AS month,
      trip_count,                  -- e.g., '2-Trips'...'10-Trips'
      repeat_passenger_count,
      SUM(repeat_passenger_count) OVER 
(PARTITION BY city_id, TO_VARCHAR(month::DATE,'YYYY-MM')) AS total_repeat_in_month
  FROM TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.DIM_REPEAT_TRIP_DISTRIBUTION_DYNAMIC
)
SELECT
    e.*,
    r.trip_count                 AS trips_bucket,
    r.repeat_passenger_count,
    IFF(NULLIF(r.total_repeat_in_month,0) IS NULL, NULL, r.repeat_passenger_count / r.total_repeat_in_month) AS bucket_share
FROM TRUSTCAB_PROD.SCH_TRUSTCAB_GOLD.EXECUTIVE_VIEW e
LEFT JOIN rtd r
  ON r.city_id = e.city_id
 AND r.month   = e.month
;


create or replace dynamic table TRUSTCAB_PROD.SCH_TRUSTCAB_GOLD.PASSENGER_VIEW(
	CITY_ID,
	CITY_NAME,
	MONTH_KEY,
	MONTH_NAME,
	START_OF_MONTH,
	TOTAL_PASSENGERS WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	NEW_PASSENGERS,
	REPEAT_PASSENGERS,
	REPEAT_PASSENGER_RATE,
	AVG_PASSENGER_RATING WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	AVG_DRIVER_RATING WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	TARGET_AVG_PASSENGER_RATING WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	TARGET_NEW_PASSENGERS WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	NEW_PAX_TARGET_DELTA WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO'),
	NEW_PAX_TARGET_ATTAINMENT WITH TAG (TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.PII_INFO='NUMERIC_INFO')
) target_lag = '10 days' refresh_mode = AUTO initialize = ON_CREATE warehouse = WH_PROD_DATA_ENGINEER
 as
WITH P AS (
  SELECT
    fps.CITY_ID,
    dc.CITY_NAME,
    fps.MONTH                    AS MONTH_KEY,
    dd.MONTH_NAME,
    dd.START_OF_MONTH,
    SUM(fps.NEW_PASSENGERS)      AS NEW_PASSENGERS,
   
 SUM(fps.REPEAT_PASSENGER)   AS REPEAT_PASSENGERS,
    SUM(fps.TOTAL_PASSENGERS)    AS TOTAL_PASSENGERS
  FROM TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.FACT_PASSENGER_SUMMARY_DYNAMIC fps
  JOIN TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.DIM_CITY_DYNAMIC dc ON dc.CITY_ID = fps.CITY_ID
  JOIN TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.DIM_DATE_DYNAMIC dd ON dd.start_of_month   = fps.MONTH
  GROUP BY 1,2,3,4,5
),
RATINGS AS (
  /* Average passenger & driver ratings from trips at the same grain */
  SELECT
    ft.CITY_ID,
    dd.start_of_month AS MONTH_KEY,
    AVG(ft.PASSENGER_RATING) AS AVG_PASSENGER_RATING,
    AVG(ft.DRIVER_RATING)    AS AVG_DRIVER_RATING
  FROM TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.FACT_TRIPS_DYNAMIC ft
  JOIN TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.DIM_DATE_DYNAMIC dd ON dd.start_of_month = ft.DATE
  GROUP BY 1,2
),
TARGETS 
AS (
  SELECT
    n.CITY_ID,
    n.MONTH                           AS MONTH_KEY,
    n.TARGET_NEW_PASSENGERS,
    t.TOTAL_TARGET_TRIPS
  FROM TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.MONTHLY_TARGET_NEW_PASSENGERS_DYNAMIC n
  LEFT JOIN TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.MONTHLY_TARGET_TRIPS_DYNAMIC t
    ON t.CITY_ID = n.CITY_ID AND t.MONTH = n.MONTH
),
RATING_TARGET AS (
  SELECT
    CITY_ID,
    TARGET_AVG_PASSENGER_RATING
  FROM TRUSTCAB_PROD.SCH_TRUSTCAB_SILVER.CITY_TARGET_PASSENGER_RATING_DYNAMIC
)
SELECT
  p.CITY_ID, p.CITY_NAME,
  p.MONTH_KEY, p.MONTH_NAME, p.START_OF_MONTH,

  /* Core Passenger KPIs */
  p.TOTAL_PASSENGERS,
  p.NEW_PASSENGERS,
 
 p.REPEAT_PASSENGERS,
  IFF(NULLIF(p.TOTAL_PASSENGERS,0) IS NULL, NULL,
      p.REPEAT_PASSENGERS / NULLIF(p.TOTAL_PASSENGERS,0)) AS REPEAT_PASSENGER_RATE,

  r.AVG_PASSENGER_RATING,
  r.AVG_DRIVER_RATING,
  rt.TARGET_AVG_PASSENGER_RATING,

  /* Targets & performance (for red/green overlays) */
  tgt.TARGET_NEW_PASSENGERS,
  (p.NEW_PASSENGERS - tgt.TARGET_NEW_PASSENGERS)            AS NEW_PAX_TARGET_DELTA,
  IFF(tgt.TARGET_NEW_PASSENGERS IS NULL, NULL,
      p.NEW_PASSENGERS / NULLIF(tgt.TARGET_NEW_PASSENGERS,0)) AS NEW_PAX_TARGET_ATTAINMENT

FROM P p
LEFT JOIN RATINGS r       ON r.CITY_ID = p.CITY_ID AND r.MONTH_KEY = p.MONTH_KEY
LEFT JOIN TARGETS tgt     ON tgt.CITY_ID = p.CITY_ID AND tgt.MONTH_KEY = p.MONTH_KEY
LEFT 
JOIN RATING_TARGET rt ON rt.CITY_ID = p.CITY_ID
;