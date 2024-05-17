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

# Script generated for node moodle_course
moodle_course_node1711049045454 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_course", transformation_ctx="moodle_course_node1711049045454")

# Script generated for node canvas_course
canvas_course_node1711049116646 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_course_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/", 
    transformation_ctx="canvas_course_node1711049116646")


# Script generated for node redshift-study
redshiftstudy_node1711997361287 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.study",
        "connectionName": "Redshift Cluster Apify"},
    transformation_ctx="redshiftstudy_node1711997361287")

# Script generated for node select-study
selectstudy_node1711997392950 = SelectFields.apply(frame=redshiftstudy_node1711997361287, paths=["id_internal_study", "id_integration_study"], transformation_ctx="selectstudy_node1711997392950")



# Script generated for node Select Fields
SelectFields_node1711049263255 = SelectFields.apply(frame=moodle_course_node1711049045454, paths=["idnumber", "fullname", "category", "timemodified"], transformation_ctx="SelectFields_node1711049263255")

# Script generated for node redshift-join
redshiftjoin_node1711997561351 = Join.apply(frame1=SelectFields_node1711049263255, frame2=selectstudy_node1711997392950, keys1=["category"], keys2=["id_internal_study"], transformation_ctx="redshiftjoin_node1711997561351")


selectstudy_node17119973921000 = SelectFields.apply(frame=redshiftjoin_node1711997561351, paths=["idnumber","fullname", "category", "timemodified", "id_integration_study"], transformation_ctx="selectstudy_node17119973921000")



# Script generated for node Select Fields
SelectFields_node1711049427938 = SelectFields.apply(frame=canvas_course_node1711049116646, paths=["sis_source_id", "name", "account_id", "created_at"], transformation_ctx="SelectFields_node1711049427938")

# Script generated for node redshift-join
redshiftjoin_node17119975613510 = Join.apply(frame1=SelectFields_node1711049427938, frame2=selectstudy_node1711997392950, keys1=["account_id"], keys2=["id_internal_study"], transformation_ctx="redshiftjoin_node17119975613510")


selectstudy_node171199739210000 = SelectFields.apply(frame=redshiftjoin_node17119975613510, paths=["sis_source_id", "name", "account_id", "created_at", "id_integration_study"], transformation_ctx="selectstudy_node171199739210000")



#filtrando las listas y haciendo la consulta sql

filter_idnumber_node1711640084734 = Filter.apply(frame=selectstudy_node17119973921000, f=lambda row: (bool(re.match("^([^|]+)\|([^|]+)\|([^_]+)__([^|]+)", row["idnumber"])) and row["timemodified"] >= 1672531200), transformation_ctx="filter_idnumber_node1711640084734")


SqlQuery0 = '''
SELECT 
    SUBSTRING_INDEX(SUBSTRING_INDEX(idnumber, '|', 2), '|', -1) AS idnumber,
    fullname,
    id_integration_study
    from myDataSource
    where idnumber != ''
'''
sql_query_sis_id_format_node1711641244591 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":filter_idnumber_node1711640084734}, transformation_ctx = "sql_query_sis_id_format_node1711641244591")


# Script generated for node Change Schema
ChangeSchema_node1711049331034 = ApplyMapping.apply(frame=sql_query_sis_id_format_node1711641244591, mappings=[("idnumber", "string", "id_integration_subject", "string"), ("id_integration_study", "string", "id_integration_study", "string"), ("fullname", "string", "description", "string")], transformation_ctx="ChangeSchema_node1711049331034")


#filtrando las listas y haciendo la consulta sql en canvas

filter_idnumber_node1711640084790 = Filter.apply(frame=selectstudy_node171199739210000, f=lambda row: (bool(re.match("^([^|]+)\|([^|]+)\|([^_]+)__([^|]+)", row["sis_source_id"]))) , transformation_ctx="filter_idnumber_node1711640084790")


SqlQuery1 = '''
SELECT 
    SUBSTRING_INDEX(SUBSTRING_INDEX(sis_source_id, '|', 2), '|', -1) AS sis_source_id,
    name,
    id_integration_study
    from myDataSource
    where DATE(created_at) BETWEEN '2023-01-01' AND current_date()
'''
sql_query_sis_id_format_node1711641244550 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":filter_idnumber_node1711640084790}, transformation_ctx = "sql_query_sis_id_format_node1711641244550")


# Script generated for node Change Schema
ChangeSchema_node1711049512094 = ApplyMapping.apply(frame=sql_query_sis_id_format_node1711641244550, mappings=[("sis_source_id", "string", "id_integration_subject", "string"), ("id_integration_study", "string", "id_integration_study", "string"), ("name", "string", "description", "string")], transformation_ctx="ChangeSchema_node1711049512094")

# Script generated for node Union
Union_node1711049581168 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": ChangeSchema_node1711049512094, "source2": ChangeSchema_node1711049331034}, transformation_ctx = "Union_node1711049581168")



# Script generated for node Amazon Redshift
AmazonRedshift_node1711049602399 = glueContext.write_dynamic_frame.from_options(
    frame=Union_node1711049581168, connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.subject", "connectionName": "Redshift Cluster Apify", "preactions": "CREATE TABLE IF NOT EXISTS public.subject (id_integration_subject VARCHAR,  id_integration_study VARCHAR,  description VARCHAR);"}, transformation_ctx="AmazonRedshift_node1711049602399")

job.commit()

# Comentarios

# Connections => mysql Moodle test,  Redshift cluster apify, redshift micampues

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED

