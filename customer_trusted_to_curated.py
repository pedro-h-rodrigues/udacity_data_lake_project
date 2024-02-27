import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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
customer_trusted_node1708907319708 = glueContext.create_dynamic_frame.from_catalog(
    database="pedro-datalake",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1708907319708",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1708907321055 = glueContext.create_dynamic_frame.from_catalog(
    database="pedro-datalake",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1708907321055",
)

# Script generated for node inner_join
SqlQuery1733 = """
select

customername,
email,
phone,
birthday,
serialnumber,
registrationdate,
lastupdatedate,
sharewithresearchasofdate,
sharewithpublicasofdate,
sharewithfriendsasofdate
from customer_trusted
inner join accelerometer_trusted
on customer_trusted.email == accelerometer_trusted.user
"""
inner_join_node1708907341589 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1733,
    mapping={
        "accelerometer_trusted": accelerometer_trusted_node1708907321055,
        "customer_trusted": customer_trusted_node1708907319708,
    },
    transformation_ctx="inner_join_node1708907341589",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1708990012350 = DynamicFrame.fromDF(
    inner_join_node1708907341589.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1708990012350",
)

# Script generated for node customer_curated
customer_curated_node1708907520472 = glueContext.getSink(
    path="s3://pedro-glue-athena-project/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1708907520472",
)
customer_curated_node1708907520472.setCatalogInfo(
    catalogDatabase="pedro-datalake", catalogTableName="customer_curated"
)
customer_curated_node1708907520472.setFormat("json")
customer_curated_node1708907520472.writeFrame(DropDuplicates_node1708990012350)
job.commit()
