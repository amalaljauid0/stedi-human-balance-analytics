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

accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-amal/accelerometer/landing/"],
        "recurse": True
    },
    transformation_ctx="accelerometer_landing"
)

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

acc_df = accelerometer_landing.toDF()
cust_df = customer_trusted.toDF()

acc_trusted_df = acc_df.join(cust_df, acc_df.user == cust_df.email, "inner").select(acc_df["user"], acc_df["timestamp"], acc_df["x"], acc_df["y"], acc_df["z"])
acc_trusted = DynamicFrame.fromDF(acc_trusted_df, glueContext, "acc_trusted")

glueContext.write_dynamic_frame.from_options(
    frame=acc_trusted,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-amal/accelerometer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="acc_trusted_sink"
)

job.commit()
