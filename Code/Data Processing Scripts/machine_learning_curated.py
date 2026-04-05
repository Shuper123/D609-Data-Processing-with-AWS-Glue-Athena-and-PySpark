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
STEP_TRUSTED_INPUT = f"{S3_BUCKET}/step_trainer/trusted/"
ACCEL_TRUSTED_INPUT = f"{S3_BUCKET}/accelerometer/trusted/"
OUTPUT_PATH = f"{S3_BUCKET}/step_trainer/curated/"

# Data Processing
step_trusted = spark.read.json(f"{S3_BUCKET}/step_trainer/trusted/")
accel_trusted = spark.read.json(f"{S3_BUCKET}/accelerometer/trusted/")

ml_curated_df = step_trusted.join(accel_trusted, step_trusted.sensorReadingTime == accel_trusted.timeStamp, "inner").distinct()

ml_curated_dyf = DynamicFrame.fromDF(ml_curated_df, glueContext, "ml_curated_dyf")
glueContext.write_dynamic_frame.from_options(
    frame=ml_curated_dyf,
    connection_type="s3",
    connection_options={"path": f"{S3_BUCKET}/step_trainer/curated/", "enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"},
    format="json",
    transformation_ctx="ml_curated_sink"
)

job.commit()