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
ACCEL_INPUT = f"{S3_BUCKET}/accelerometer/landing/"
CUST_TRUSTED_INPUT = f"{S3_BUCKET}/customer/trusted/"
OUTPUT_PATH = f"{S3_BUCKET}/accelerometer/trusted/"

# Data Processing
accel_landing = spark.read.json(f"{S3_BUCKET}/accelerometer/landing/")
customer_trusted = spark.read.json(f"{S3_BUCKET}/customer/trusted/")

accel_trusted_df = accel_landing.join(customer_trusted, accel_landing.user == customer_trusted.email, "inner")
accel_trusted_df = accel_trusted_df.select("user", "timeStamp", "x", "y", "z").distinct()

accel_trusted_dyf = DynamicFrame.fromDF(accel_trusted_df, glueContext, "accel_trusted_dyf")
glueContext.write_dynamic_frame.from_options(
    frame=accel_trusted_dyf,
    connection_type="s3",
    connection_options={"path": f"{S3_BUCKET}/accelerometer/trusted/", "enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"},
    format="json",
    transformation_ctx="accel_trusted_sink"
)

job.commit()