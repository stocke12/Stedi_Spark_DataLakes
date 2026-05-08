import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1778264348173 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1778264348173")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1778264381920 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1778264381920")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT
    ct.customername,
    ct.email,
    ct.phone,
    ct.birthday,
    ct.serialnumber,
    ct.registrationdate,
    ct.lastupdatedate,
    ct.sharewithresearchasofdate,
    ct.sharewithpublicasofdate,
    ct.sharewithfriendsasofdate
FROM customer_trusted ct
JOIN accelerometer_trusted at ON ct.email = at.user
'''
SQLQuery_node1778264412954 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":CustomerTrusted_node1778264348173, "accelerometer_trusted":AccelerometerTrusted_node1778264381920}, transformation_ctx = "SQLQuery_node1778264412954")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1778264412954, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1778263091730", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1778264496460 = glueContext.getSink(path="s3://stedi-lakehouse-stockton/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1778264496460")
CustomerCurated_node1778264496460.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1778264496460.setFormat("glueparquet", compression="snappy")
CustomerCurated_node1778264496460.writeFrame(SQLQuery_node1778264412954)
job.commit()
