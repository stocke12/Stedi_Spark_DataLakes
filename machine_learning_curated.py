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

# Script generated for node Customer Curated
CustomerCurated_node1778266598013 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1778266598013")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1778266549472 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1778266549472")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1778266575934 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1778266575934")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    st.sensorreadingtime,
    st.serialnumber,
    st.distancefromobject,
    at.timestamp as accelerometer_timestamp,
    at.x,
    at.y,
    at.z
FROM step_trainer_trusted st
JOIN accelerometer_trusted at ON st.sensorreadingtime = at.timestamp
JOIN customer_curated cc ON st.serialnumber = cc.serialnumber
'''
SQLQuery_node1778266621142 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":CustomerCurated_node1778266598013, "step_trainer_trusted":StepTrainerTrusted_node1778266549472, "accelerometer_trusted":AccelerometerTrusted_node1778266575934}, transformation_ctx = "SQLQuery_node1778266621142")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1778266621142, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1778266054834", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1778267324099 = glueContext.getSink(path="s3://stedi-lakehouse-stockton/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1778267324099")
MachineLearningCurated_node1778267324099.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1778267324099.setFormat("glueparquet", compression="snappy")
MachineLearningCurated_node1778267324099.writeFrame(SQLQuery_node1778266621142)
job.commit()
