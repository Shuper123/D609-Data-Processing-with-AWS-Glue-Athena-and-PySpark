import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_BUCKET = "s3://d509-jshupe-stedi-data"
INPUT_PATH = f"{S3_BUCKET}/customer/landing/"
OUTPUT_PATH = f"{S3_BUCKET}/customer/trusted/"

# Data Processing
customer_landing = spark.read.json(INPUT_PATH)
customer_trusted_df = customer_landing.filter(customer_landing.shareWithResearchAsOfDate.isNotNull()).distinct()

customer_trusted_dyf = DynamicFrame.fromDF(customer_trusted_df, glueContext, "customer_trusted_dyf")
glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted_dyf,
    connection_type="s3",
    connection_options={"path": OUTPUT_PATH, "enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"},
    format="json",
    transformation_ctx="customer_trusted_sink"
)

job.commit()