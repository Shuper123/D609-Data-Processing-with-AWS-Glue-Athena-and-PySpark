# Data Processing with AWS Glue Athena and PySpark

## Overview
This project defines a Spark-based Data Lakehouse architecture on AWS. The objective was to process raw JSON data from the STEDI Step Trainer (a motion-sensing device) and move it through a structured pipeline: 
**Landing -> Trusted -> Curated**. 

The primary focus was maintaining data integrity and privacy. I used PySpark to filter out PII and ensure that only sensor data from customers who explicitly opted into research consent was used for downstream analytics and machine learning.

## Tech Stack
* **AWS Glue (PySpark):** Executed the ETL "factory" jobs, including complex joins and PII filtering.
* **AWS S3:** Served as the storage layer for the three data zones (Landing, Trusted, and Curated).
* **AWS Athena:** Used to define the Data Catalog and run SQL verification queries.
* **AWS CLI:** Utilized for high-speed data uploads to S3.

## Project Structure
* **`/scripts`**: Contains 5 Python Glue jobs and 8 SQL DDL scripts used for table registration.
* **`/screenshots`**: Visual evidence of the Athena queries, including counts and null-value verification.

## Key Engineering Logic
* **PII Filtering:** The `customer_landing_to_trusted` job drops records missing a `shareWithResearchAsOfDate`, ensuring only consented data moves forward.
* **Dynamic Catalog Updates:** Glue jobs were configured with `enableUpdateCatalog: True` to ensure the Glue Data Catalog stays in sync with S3 during every run.
