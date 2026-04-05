CREATE EXTERNAL TABLE IF NOT EXISTS `step_trainer_trusted` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://d509-jshupe-stedi-data/step_trainer/trusted/';