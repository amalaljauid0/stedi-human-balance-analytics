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

step_trainer_trusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-balance-amal/step_trainer/trusted/"],
        "recurse": True
    },
    transformation_ctx="step_trainer_trusted"
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

st_df = step_trainer_trusted.toDF()
acc_df = accelerometer_trusted.toDF()

st_df.createOrReplaceTempView("step_trainer_trusted")
acc_df.createOrReplaceTempView("accelerometer_trusted")

ml_curated_df = spark.sql("""
    SELECT st.*, acc.user, acc.x, acc.y, acc.z
    FROM step_trainer_trusted st
    JOIN accelerometer_trusted acc
    ON st.sensorReadingTime = acc.timestamp
""")

ml_curated = DynamicFrame.fromDF(ml_curated_df, glueContext, "ml_curated")

glueContext.write_dynamic_frame.from_options(
    frame=ml_curated,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-balance-amal/machine_learning/curated/",
        "partitionKeys": []
    },
    transformation_ctx="ml_curated_sink"
)

job.commit()
