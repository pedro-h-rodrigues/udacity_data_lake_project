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

# Script generated for node customer_trusted
customer_trusted_node1708896695913 = glueContext.create_dynamic_frame.from_catalog(
    database="pedro-datalake",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1708896695913",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1708896802138 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://pedro-glue-athena-project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1708896802138",
)

# Script generated for node SQL Query
SqlQuery1538 = """
select 
user,
timestamp,
x,
y,
z
from accelerometer_landing
inner join customer_trusted
on accelerometer_landing.user = customer_trusted.email
"""
SQLQuery_node1708897022768 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1538,
    mapping={
        "accelerometer_landing": accelerometer_landing_node1708896802138,
        "customer_trusted": customer_trusted_node1708896695913,
    },
    transformation_ctx="SQLQuery_node1708897022768",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1708897193165 = glueContext.getSink(
    path="s3://pedro-glue-athena-project/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1708897193165",
)
accelerometer_trusted_node1708897193165.setCatalogInfo(
    catalogDatabase="pedro-datalake", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1708897193165.setFormat("json")
accelerometer_trusted_node1708897193165.writeFrame(SQLQuery_node1708897022768)
job.commit()
