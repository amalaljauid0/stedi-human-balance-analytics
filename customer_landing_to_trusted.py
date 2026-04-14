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

customer_landing = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-amal/customer/landing/"],
        "recurse": True
    },
    transformation_ctx="customer_landing"
)

df = customer_landing.toDF()
df_trusted = df.filter(df.shareWithResearchAsOfDate.isNotNull() & (df.shareWithResearchAsOfDate != 0))
customer_trusted = DynamicFrame.fromDF(df_trusted, glueContext, "customer_trusted")

glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-amal/customer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="customer_trusted_sink"
)

job.commit()
