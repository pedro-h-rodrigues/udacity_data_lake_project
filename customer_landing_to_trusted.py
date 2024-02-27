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

# Script generated for node customer_landing
customer_landing_node1708893198250 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://pedro-glue-athena-project/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1708893198250",
)

# Script generated for node filter_null_sharewithresearchasofdate
SqlQuery1427 = """
select * from myDataSource
where not(isnull(shareWithResearchAsOfDate))
"""
filter_null_sharewithresearchasofdate_node1708893308944 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1427,
    mapping={"myDataSource": customer_landing_node1708893198250},
    transformation_ctx="filter_null_sharewithresearchasofdate_node1708893308944",
)

# Script generated for node Amazon S3
AmazonS3_node1708893394147 = glueContext.getSink(
    path="s3://pedro-glue-athena-project/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1708893394147",
)
AmazonS3_node1708893394147.setCatalogInfo(
    catalogDatabase="pedro-datalake", catalogTableName="customer_trusted"
)
AmazonS3_node1708893394147.setFormat("json")
AmazonS3_node1708893394147.writeFrame(
    filter_null_sharewithresearchasofdate_node1708893308944
)
job.commit()
