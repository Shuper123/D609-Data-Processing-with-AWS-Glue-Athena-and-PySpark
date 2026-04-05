import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# AWS Glue setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 Configuration
S3_BUCKET = "s3://d509-jshupe-stedi-data" 
STEP_INPUT = f"{S3_BUCKET}/step_trainer/landing/"
CUST_CURATED_INPUT = f"{S3_BUCKET}/customer/curated/"
OUTPUT_PATH = f"{S3_BUCKET}/step_trainer/trusted/"

# Data Processing

step_landing = spark.read.json(f"{S3_BUCKET}/step_trainer/landing/")
customer_curated = spark.read.json(f"{S3_BUCKET}/customer/curated/")

step_trusted_df = step_landing.join(customer_curated, step_landing.serialNumber == customer_curated.serialNumber, "inner")

step_trusted_df = step_trusted_df.select(
    step_landing["sensorReadingTime"], 
    step_landing["serialNumber"], 
    step_landing["distanceFromObject"]
).distinct()

step_trusted_dyf = DynamicFrame.fromDF(step_trusted_df, glueContext, "step_trusted_dyf")
glueContext.write_dynamic_frame.from_options(
    frame=step_trusted_dyf,
    connection_type="s3",
    connection_options={"path": f"{S3_BUCKET}/step_trainer/trusted/", "enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"},
    format="json",
    transformation_ctx="step_trusted_sink"
)

job.commit()