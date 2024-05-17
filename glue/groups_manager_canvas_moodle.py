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

# Script generated for node canvas_groups
canvas_groups_node1712031552080 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_course_section_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/",
    transformation_ctx="canvas_groups_node1712031552080")

# Script generated for node moodle_groups
moodle_groups_node1711070523845 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_groups", transformation_ctx="moodle_groups_node1711070523845")

# Script generated for node moodle_course
moodle_course_node1712032926080 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_course", transformation_ctx="moodle_course_node1712032926080")

# Script generated for node canvas_course
canvas_course_node1712032434079 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_course_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/",
    transformation_ctx="canvas_course_node1712032434079")

# Script generated for node Select Fields
SelectFields_node1712031631020 = SelectFields.apply(frame=canvas_groups_node1712031552080, paths=["course_id", "sis_source_id", "name"], transformation_ctx="SelectFields_node1712031631020")

# Script generated for node Select Fields
SelectFields_node1711070560116 = SelectFields.apply(frame=moodle_groups_node1711070523845, paths=["idnumber", "courseid", "name"], transformation_ctx="SelectFields_node1711070560116")

# Script generated for node Select Fields
SelectFields_node1712033011406 = SelectFields.apply(frame=moodle_course_node1712032926080, paths=["id", "idnumber"], transformation_ctx="SelectFields_node1712033011406")

# Script generated for node Select Fields
SelectFields_node1712032613203 = SelectFields.apply(frame=canvas_course_node1712032434079, paths=["sis_source_id", "id"], transformation_ctx="SelectFields_node1712032613203")

# Script generated for node Filter
Filter_node1712032834533 = Filter.apply(frame=SelectFields_node1712031631020, f=lambda row: (bool(re.match("^\d+$", row["sis_source_id"]))), transformation_ctx="Filter_node1712032834533")

# Script generated for node Filter
Filter_node1712032877808 = Filter.apply(frame=SelectFields_node1711070560116, f=lambda row: (bool(re.match("^\d+$", row["idnumber"]))), transformation_ctx="Filter_node1712032877808")

# Script generated for node Filter
Filter_node1712033033864 = Filter.apply(frame=SelectFields_node1712033011406, f=lambda row: (bool(re.match("^([^|]+)\|([^|]+)\|([^_]+)__([^|]+)", row["idnumber"]))), transformation_ctx="Filter_node1712033033864")

# Script generated for node Filter
Filter_node1712032769559 = Filter.apply(frame=SelectFields_node1712032613203, f=lambda row: (bool(re.match("^([^|]+)\|([^|]+)\|([^_]+)__([^|]+)", row["sis_source_id"]))), transformation_ctx="Filter_node1712032769559")

# Script generated for node Change Schema
ChangeSchema_node1712034053617 = ApplyMapping.apply(frame=Filter_node1712032834533, mappings=[("course_id", "long", "course_id", "long"), ("sis_source_id", "string", "id_integration_manager", "string"), ("name", "string", "description", "string")], transformation_ctx="ChangeSchema_node1712034053617")

# Script generated for node SQL Query
SqlQuery2800 = '''
SELECT 
    SUBSTRING_INDEX(SUBSTRING_INDEX(idnumber, '|', 2), '|', -1) AS id_integration_subject,
    id
    from myDataSource

'''
SQLQuery_node1712033069208 = sparkSqlQuery(glueContext, query = SqlQuery2800, mapping = {"myDataSource":Filter_node1712033033864}, transformation_ctx = "SQLQuery_node1712033069208")

# Script generated for node SQL Query
SqlQuery2799 = '''
SELECT 
    SUBSTRING_INDEX(SUBSTRING_INDEX(sis_source_id, '|', 2), '|', -1) AS id_integration_subject,
    id
    from myDataSource

'''
SQLQuery_node1712033423820 = sparkSqlQuery(glueContext, query = SqlQuery2799, mapping = {"myDataSource":Filter_node1712032769559}, transformation_ctx = "SQLQuery_node1712033423820")

# Script generated for node Join
Join_node1712034096417 = Join.apply(frame1=ChangeSchema_node1712034053617, frame2=SQLQuery_node1712033423820, keys1=["course_id"], keys2=["id"], transformation_ctx="Join_node1712034096417")

# Script generated for node Join
Join_node1712033238463 = Join.apply(frame1=Filter_node1712032877808, frame2=SQLQuery_node1712033069208, keys1=["courseid"], keys2=["id"], transformation_ctx="Join_node1712033238463")

# Script generated for node Change Schema
ChangeSchema_node1712034173209 = ApplyMapping.apply(frame=Join_node1712034096417, mappings=[("id_integration_manager", "string", "id_integration_manager", "string"), ("sis_source_id", "string", "id_integration_subject", "string")], transformation_ctx="ChangeSchema_node1712034173209")

# Script generated for node Change Schema
ChangeSchema_node1712033472403 = ApplyMapping.apply(frame=Join_node1712033238463, mappings=[("idnumber", "string", "id_integration_manager", "string"), ("id_integration_subject", "string", "id_integration_subject", "string")], transformation_ctx="ChangeSchema_node1712033472403")

# Script generated for node Union
Union_node1712034282976 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": ChangeSchema_node1712033472403, "source2": ChangeSchema_node1712034173209}, transformation_ctx = "Union_node1712034282976")

# Script generated for node Amazon Redshift
AmazonRedshift_node1712034303002 = glueContext.write_dynamic_frame.from_options(frame=Union_node1712034282976, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.group_manager", "connectionName": "Redshift Cluster Apify", "preactions": "CREATE TABLE IF NOT EXISTS public.group_manager (id_integration_manager VARCHAR,  id_integration_subject VARCHAR);"}, transformation_ctx="AmazonRedshift_node1712034303002")

job.commit()

# Comentarios

# Connections => mysql Moodle test,  Redshift cluster apify, redshift micampues

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED