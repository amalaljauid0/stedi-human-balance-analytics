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

step_trainer_landing = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-amal/step_trainer/landing/"],
        "recurse": True
    },
    transformation_ctx="step_trainer_landing"
)

customers_curated = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-amal/customer/curated/"],
        "recurse": True
    },
    transformation_ctx="customers_curated"
)

st_df = step_trainer_landing.toDF()
cust_df = customers_curated.toDF()

st_df.createOrReplaceTempView("step_trainer")
cust_df.createOrReplaceTempView("customers")

step_trainer_trusted_df = spark.sql("""
    SELECT st.*
    FROM step_trainer st
    WHERE st.serialNumber IN (SELECT serialNumber FROM customers)
""")

step_trainer_trusted = DynamicFrame.fromDF(step_trainer_trusted_df, glueContext, "step_trainer_trusted")

glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_trusted,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-amal/step_trainer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="step_trainer_trusted_sink"
)

job.commit()
