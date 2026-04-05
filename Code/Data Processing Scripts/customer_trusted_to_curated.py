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
CUST_TRUSTED_INPUT = f"{S3_BUCKET}/customer/trusted/"
ACCEL_TRUSTED_INPUT = f"{S3_BUCKET}/accelerometer/trusted/"
OUTPUT_PATH = f"{S3_BUCKET}/customer/curated/"

# Data Processing
customer_trusted = spark.read.json(f"{S3_BUCKET}/customer/trusted/")
accel_trusted = spark.read.json(f"{S3_BUCKET}/accelerometer/trusted/")

customer_curated_df = customer_trusted.join(accel_trusted, customer_trusted.email == accel_trusted.user, "inner")
customer_curated_df = customer_curated_df.select(customer_trusted.columns).distinct()

customer_curated_dyf = DynamicFrame.fromDF(customer_curated_df, glueContext, "customer_curated_dyf")
glueContext.write_dynamic_frame.from_options(
    frame=customer_curated_dyf,
    connection_type="s3",
    connection_options={"path": f"{S3_BUCKET}/customer/curated/", "enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"},
    format="json",
    transformation_ctx="customer_curated_sink"
)

job.commit()