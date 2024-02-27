import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1708993202215 = glueContext.create_dynamic_frame.from_catalog(
    database="pedro-datalake",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1708993202215",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1708993221499 = glueContext.create_dynamic_frame.from_catalog(
    database="pedro-datalake",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1708993221499",
)

# Script generated for node inner_join
SqlQuery1707 = """
select
sensorreadingtime,
serialnumber,
distancefromobject,
user,
x,
y,
z
from step_trainer_trusted
inner join accelerometer_trusted
on step_trainer_trusted.sensorreadingtime == accelerometer_trusted.timestamp
"""
inner_join_node1708993257115 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1707,
    mapping={
        "step_trainer_trusted": step_trainer_trusted_node1708993202215,
        "accelerometer_trusted": accelerometer_trusted_node1708993221499,
    },
    transformation_ctx="inner_join_node1708993257115",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1708993884367 = glueContext.getSink(
    path="s3://pedro-glue-athena-project/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1708993884367",
)
machine_learning_curated_node1708993884367.setCatalogInfo(
    catalogDatabase="pedro-datalake", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1708993884367.setFormat("json")
machine_learning_curated_node1708993884367.writeFrame(inner_join_node1708993257115)
job.commit()
