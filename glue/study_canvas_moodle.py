import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import lit
import re


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql("(select * from source1) UNION " + unionType + " (select * from source2)")
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1711046902876 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_account_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/", 
    transformation_ctx="AWSGlueDataCatalog_node1711046902876")


# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1710945926359 = glueContext.create_dynamic_frame.from_catalog(
    database="apify", 
    table_name="moodle_education_moodle_campusvirtual_educacion_mdl_course_categories", 
    transformation_ctx="AWSGlueDataCatalog_node1710945926359")

# Script generated for node Select Fields
SelectFields_node1711047769076 = SelectFields.apply(frame=AWSGlueDataCatalog_node1711046902876, paths=["sis_source_id", "name", "id"], transformation_ctx="SelectFields_node1711047769076")


# Script generated for node Select Fields
SelectFields_node1710945987947 = SelectFields.apply(frame=AWSGlueDataCatalog_node1710945926359, paths=["idnumber", "name", "timemodified", "id"], transformation_ctx="SelectFields_node1710945987947")




filter_idnumber_node1711640084734 = Filter.apply(frame=SelectFields_node1710945987947, f=lambda row: (bool(re.match("^([^|]+)\|([^_]+)__([^|]+)\|([^|]+)", row["idnumber"])) and row["timemodified"] >= 1672531200), transformation_ctx="filter_idnumber_node1711640084734")

SqlQuery0 = '''
SELECT 
    SUBSTRING_INDEX(idnumber, '|', 1) AS idnumber,
    name,
    id
    from moodle
'''
sql_query_sis_id_format_node1711641244591 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"moodle":filter_idnumber_node1711640084734}, transformation_ctx = "sql_query_sis_id_format_node1711641244591")


# filtros para canvas

filter_idnumber_node17116400847340 = Filter.apply(frame=SelectFields_node1711047769076, f=lambda row: (bool(re.match("^([^|]+)\|([^_]+)__([^|]+)\|([^|]+)", row["sis_source_id"]))), transformation_ctx="filter_idnumber_node17116400847340")

SqlQuery1 = '''
SELECT 
    SUBSTRING_INDEX(sis_source_id, '|', 1) AS sis_source_id,
    name,
    id
    from canvas
'''
sql_query_sis_id_format_node17116412445910 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"canvas":filter_idnumber_node17116400847340}, transformation_ctx = "sql_query_sis_id_format_node17116412445910")

# Script generated for node Change Schema
ChangeSchema_node1710946537273 = ApplyMapping.apply(frame=sql_query_sis_id_format_node1711641244591, mappings=[("idnumber", "string", "id_integration_study", "string"), ("name", "string", "description", "string"), ("id", "bigint", "id_internal_study", "bigint")], transformation_ctx="ChangeSchema_node1710946537273")


# Script generated for node Change Schema
ChangeSchema_node1711048140914 = ApplyMapping.apply(frame=sql_query_sis_id_format_node17116412445910, mappings=[("sis_source_id", "string", "id_integration_study", "string"), ("name", "string", "description", "string"), ("id", "bigint", "id_internal_study", "bigint")], transformation_ctx="ChangeSchema_node1711048140914")

# Script generated for node Union
Union_node1711048206846 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": ChangeSchema_node1710946537273, "source2": ChangeSchema_node1711048140914}, transformation_ctx = "Union_node1711048206846")


# # Script generated for node Amazon Redshift
# AmazonRedshift_node1711048230665 = glueContext.write_dynamic_frame.from_options(frame=Union_node1711048206846, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.study", "connectionName": "Redshift Cluster Apify", "preactions": "DROP TABLE IF EXISTS public.study; CREATE TABLE IF NOT EXISTS public.study (id BIGINT IDENTITY(1,1) PRIMARY KEY, description VARCHAR, id_integration_study VARCHAR, id_internal_study BIGINT, id_data_origin BIGINT );"}, transformation_ctx="AmazonRedshift_node1711048230665")

# Script generated for node Amazon Redshift
AmazonRedshift_node1711048230665 = glueContext.write_dynamic_frame.from_options(frame=Union_node1711048206846, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.study", "connectionName": "Redshift Cluster Apify", "preactions": "CREATE TABLE IF NOT EXISTS public.study (id_integration_study VARCHAR, description VARCHAR, id_internal_study BIGINT);"}, transformation_ctx="AmazonRedshift_node1711048230665")

job.commit()

# Comentarios

# Connections => mysql Moodle test,  Redshift cluster apify, redshift micampues

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED