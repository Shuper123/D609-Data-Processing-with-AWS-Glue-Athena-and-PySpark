CREATE EXTERNAL TABLE IF NOT EXISTS `machine_learning_curated` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int,
  `user` string,
  `timeStamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://d509-jshupe-stedi-data/step_trainer/curated/';