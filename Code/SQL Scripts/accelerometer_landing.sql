CREATE EXTERNAL TABLE IF NOT EXISTS `accelerometer_landing` (
  `user` string,
  `timeStamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://d509-jshupe-stedi-data/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');