CREATE EXTERNAL TABLE IF NOT EXISTS `trip_tb`(
  `vendor_id` integer,
  `passenger_count` integer,
  `pickup_longitude` float,
  `pickup_latitude` float,
  `dropoff_longitude` float,
  `dropoff_latitude` float,
  `store_and_fwd_flag` integer,
  `trip_duration` float,
  `pick_year` integer,
  `pick_month` integer,
  `pick_day` integer,
  `pick_hr` integer,
  `pick_minute` integer,
  `pick_weekday` integer,
  `pickup_dropoff_loc` integer,
  `Temp` float,
  `Precip` float,
  `snow` float,
  `Visibility` float,
  `id` string,
  `pickup_datetime` timestamp,
  `dropoff_datetime` timestamp,
  `log_trip_duration` float,
  `prediction` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1',
   "dots.in.keys" = "true"
) LOCATION '<YOUR-S3-BUCKET-HERE>';