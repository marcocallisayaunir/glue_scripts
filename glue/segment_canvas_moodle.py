import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
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
moodle_course_node1712002108282 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_course", transformation_ctx="moodle_course_node1712002108282")


# Script generated for node canvas_course
canvas_course_node1712003585579 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="micampus_micampus_public_course_dim",  redshift_tmp_dir="s3://g-redshift-temp/temporal/", transformation_ctx="canvas_course_node1712003585579")

# Script generated for node select_fields_course
select_fields_course_node1712002214738 = SelectFields.apply(frame=moodle_course_node1712002108282, paths=["idnumber"], transformation_ctx="select_fields_course_node1712002214738")

# Script generated for node select_fields_course_canvas
select_fields_course_canvas_node1712003877237 = SelectFields.apply(frame=canvas_course_node1712003585579, paths=["sis_source_id"], transformation_ctx="select_fields_course_canvas_node1712003877237")

# Script generated for node filter_segment_course
filter_segment_course_node1712002265626 = Filter.apply(frame=select_fields_course_node1712002214738, f=lambda row: (bool(re.match("^(\d+-\d+-\d+-\d+)\|(\d+)\|(\d+)__(\d+)$", row["idnumber"]))), transformation_ctx="filter_segment_course_node1712002265626")


# Script generated for node filter_course_canvas
filter_course_canvas_node1712004032121 = Filter.apply(frame=select_fields_course_canvas_node1712003877237, f=lambda row: (bool(re.match("^(\d+-\d+-\d+-\d+)\|(\d+)\|(\d+)__(\d+)$", row["sis_source_id"]))), transformation_ctx="filter_course_canvas_node1712004032121")


# Script generated for node query_sql_semeng_course
SqlQuery0 = '''
select 
SUBSTRING_INDEX(idnumber, '|', -1) AS idnumber,
SUBSTRING_INDEX(SUBSTRING_INDEX(idnumber, '|', 2), '|', -1) AS id_integration_subject
from myDataSourceCourseMoodle
'''
query_sql_semeng_course_node1712002471881 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSourceCourseMoodle":filter_segment_course_node1712002265626}, transformation_ctx = "query_sql_semeng_course_node1712002471881")

# Script generated for node query_course_canvas
SqlQuery2 = '''
select 
SUBSTRING_INDEX(sis_source_id, '|', -1) AS sis_source_id,
SUBSTRING_INDEX(SUBSTRING_INDEX(sis_source_id, '|', 2), '|', -1) AS id_integration_subject
from myDataSource
'''
query_course_canvas_node1712004217996 = sparkSqlQuery(glueContext, query = SqlQuery2, mapping = {"myDataSource":filter_course_canvas_node1712004032121}, transformation_ctx = "query_course_canvas_node1712004217996")

# Script generated for node Schema Matcher for Union
SchemaMatcherforUnion_node1712003165902 = ApplyMapping.apply(frame=query_sql_semeng_course_node1712002471881, mappings=[("idnumber", "string", "id_segment_integration", "string"), ("id_integration_subject", "string", "id_integration_subject", "string")], transformation_ctx="SchemaMatcherforUnion_node1712003165902")


# Script generated for node Change Schema
ChangeSchema_node1712004323924 = ApplyMapping.apply(frame=query_course_canvas_node1712004217996, mappings=[("sis_source_id", "string", "id_segment_integration", "string"), ("id_integration_subject", "string", "id_integration_subject", "string")], transformation_ctx="ChangeSchema_node1712004323924")


# Script generated for node union_moodle_segment
union_moodle_segment_node1712003334139 = sparkUnion(glueContext, unionType = "DISTINCT", mapping = {"source1": SchemaMatcherforUnion_node1712003165902, "source2": ChangeSchema_node1712004323924}, transformation_ctx = "union_moodle_segment_node1712003334139")

# Script generated for node redshift_segment
redshift_segment_node1712004505141 = glueContext.write_dynamic_frame.from_options(frame=union_moodle_segment_node1712003334139, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.segment", "connectionName": "Redshift Cluster Apify", "preactions": "DROP TABLE IF EXISTS public.segment; CREATE TABLE IF NOT EXISTS public.segment (id BIGINT PRIMARY KEY IDENTITY(1,1), id_segment_integration VARCHAR, id_integration_subject VARCHAR);"}, transformation_ctx="redshift_segment_node1712004505141")

job.commit()

# Comentarios

# Connections => mysql Moodle test,  Redshift cluster apify, redshift micampues

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED
