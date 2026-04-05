CREATE EXTERNAL TABLE IF NOT EXISTS `accelerometer_trusted` (
  `user` string,
  `timeStamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://d509-jshupe-stedi-data/accelerometer/trusted/';