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

# Script generated for node step_trainer_landing
step_trainer_landing_node1708991823648 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://pedro-glue-athena-project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1708991823648",
)

# Script generated for node customer_trusted
customer_trusted_node1708991964036 = glueContext.create_dynamic_frame.from_catalog(
    database="pedro-datalake",
    table_name="customer_curated",
    transformation_ctx="customer_trusted_node1708991964036",
)

# Script generated for node inner_join
SqlQuery1503 = """
select
s.sensorreadingtime,
s.serialnumber,
s.distancefromobject
from step_trainer_landing s
inner join customer_trusted c
on s.serialnumber == c.serialnumber
"""
inner_join_node1708992062898 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1503,
    mapping={
        "step_trainer_landing": step_trainer_landing_node1708991823648,
        "customer_trusted": customer_trusted_node1708991964036,
    },
    transformation_ctx="inner_join_node1708992062898",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1708992322999 = glueContext.getSink(
    path="s3://pedro-glue-athena-project/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1708992322999",
)
step_trainer_trusted_node1708992322999.setCatalogInfo(
    catalogDatabase="pedro-datalake", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1708992322999.setFormat("json")
step_trainer_trusted_node1708992322999.writeFrame(inner_join_node1708992062898)
job.commit()
