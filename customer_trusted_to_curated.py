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

customer_trusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-amal/customer/trusted/"],
        "recurse": True
    },
    transformation_ctx="customer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-amal/accelerometer/trusted/"],
        "recurse": True
    },
    transformation_ctx="accelerometer_trusted"
)

cust_df = customer_trusted.toDF()
acc_df = accelerometer_trusted.toDF()

customers_curated_df = cust_df.join(acc_df, cust_df.email == acc_df.user, "inner").select(cust_df["*"]).distinct()
customers_curated = DynamicFrame.fromDF(customers_curated_df, glueContext, "customers_curated")

glueContext.write_dynamic_frame.from_options(
    frame=customers_curated,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-amal/customer/curated/",
        "partitionKeys": []
    },
    transformation_ctx="customers_curated_sink"
)

job.commit()
