import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
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

# Script generated for node Amazon S3
AmazonS3_node1667857082712 = glueContext.create_dynamic_frame.from_catalog(
    database="sample-pii-data",
    table_name="vig_pii_test",
    transformation_ctx="AmazonS3_node1667857082712",
)

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
detected_df = entity_detector.detect(
    AmazonS3_node1667857082712, ["CREDIT_CARD"], "DetectedEntities"
)


def replace_cell(original_cell_value, sorted_reverse_start_end_tuples):
    if sorted_reverse_start_end_tuples:
        for entity in sorted_reverse_start_end_tuples:
            to_mask_value = original_cell_value[entity[0] : entity[1]]
            original_cell_value = original_cell_value.replace(
                to_mask_value, "CONFIDENTIAL"
            )
    return original_cell_value


def row_pii(column_name, original_cell_value, detected_entities):
    if column_name in detected_entities.keys():
        entities = detected_entities[column_name]
        start_end_tuples = map(
            lambda entity: (entity["start"], entity["end"]), entities
        )
        sorted_reverse_start_end_tuples = sorted(
            start_end_tuples, key=lambda start_end: start_end[1], reverse=True
        )
        return replace_cell(original_cell_value, sorted_reverse_start_end_tuples)
    return original_cell_value


row_pii_udf = udf(row_pii, StringType())


def recur(df, remaining_keys):
    if len(remaining_keys) == 0:
        return df
    else:
        head = remaining_keys[0]
        tail = remaining_keys[1:]
        modified_df = df.withColumn(
            head, row_pii_udf(lit(head), head, "DetectedEntities")
        )
        return recur(modified_df, tail)


keys = AmazonS3_node1667857082712.toDF().columns
updated_masked_df = recur(detected_df.toDF(), keys)
updated_masked_df = updated_masked_df.drop("DetectedEntities")

DetectSensitiveData_node1667857110976 = DynamicFrame.fromDF(
    updated_masked_df, glueContext, "updated_masked_df"
)

# Script generated for node SQL Query
SqlQuery656 = """
SELECT * FROM Customer_Info ORDER BY lname ASC;
"""
SQLQuery_node1667857359353 = sparkSqlQuery(
    glueContext,
    query=SqlQuery656,
    mapping={"Customer_Info": DetectSensitiveData_node1667857110976},
    transformation_ctx="SQLQuery_node1667857359353",
)

# Script generated for node PostgreSQL
PostgreSQL_node1667857565353 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1667857359353,
    database="pii-data",
    table_name="pii_data_public_pii",
    transformation_ctx="PostgreSQL_node1667857565353",
)

job.commit()
