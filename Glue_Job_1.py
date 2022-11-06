import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue import DynamicFrame

def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(
                ctx,
                field.dataType,
                new_path + field.name,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(
                ctx,
                schema.elementType,
                path,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split(".")[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set(
                    [
                        item.strip() if isinstance(item, str) else item
                        for item in distinct_
                    ]
                )
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif (
            isinstance(schema, IntegerType)
            or isinstance(schema, LongType)
            or isinstance(schema, DoubleType)
        ):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output


def drop_nulls(
    glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx
) -> DynamicFrame:
    nullColumns = _find_null_fields(
        frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame
    )
    return DropFields.apply(
        frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx
    )


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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1667698747187 = glueContext.create_dynamic_frame.from_catalog(
    database="ecuador-test-csv",
    table_name="ecuador_food_prices_bucket",
    transformation_ctx="AWSGlueDataCatalog_node1667698747187",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1667698771596 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1667698747187,
    mappings=[
        ("admin1", "string", "province", "string"),
        ("admin2", "string", "city", "string"),
        ("market", "string", "market", "string"),
        ("unit", "string", "unit", "string"),
        ("currency", "string", "currency", "string"),
        ("price", "double", "price", "decimal"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1667698771596",
)

# Script generated for node Drop Null Fields
DropNullFields_node1667699087579 = drop_nulls(
    glueContext,
    frame=ChangeSchemaApplyMapping_node1667698771596,
    nullStringSet={""},
    nullIntegerSet={},
    transformation_ctx="DropNullFields_node1667699087579",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT * FROM ecuador_table ORDER BY price ASC LIMIT 10;
"""
SQLQuery_node1667699103145 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"ecuador_table": DropNullFields_node1667699087579},
    transformation_ctx="SQLQuery_node1667699103145",
)

# Script generated for node PostgreSQL
PostgreSQL_node1667700484035 = glueContext.write_dynamic_frame.from_catalog(
    frame=SQLQuery_node1667699103145,
    database="rds",
    table_name="ecuador_food_public_ecuador_food",
    transformation_ctx="PostgreSQL_node1667700484035",
)

job.commit()
