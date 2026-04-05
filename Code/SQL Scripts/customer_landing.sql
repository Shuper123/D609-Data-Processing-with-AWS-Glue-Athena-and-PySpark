CREATE EXTERNAL TABLE IF NOT EXISTS `customer_landing` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDay` string,
  `serialNumber` string,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint,
  `shareWithFriendsAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://d509-jshupe-stedi-data/customer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');