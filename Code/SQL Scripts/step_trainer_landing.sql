CREATE EXTERNAL TABLE IF NOT EXISTS `step_trainer_landing` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://d509-jshupe-stedi-data/step_trainer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');