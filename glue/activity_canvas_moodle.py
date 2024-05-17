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
AWSGlueDataCatalog_node1712236954814 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_assignment_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/",  # campo requerido en redshift
    transformation_ctx="AWSGlueDataCatalog_node1712236954814")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node187962810 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_submission_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/",  # campo requerido en redshift
    transformation_ctx="AWSGlueDataCatalog_node187962810")

# Script generated for node Select Fields
SelectFields_node1712237000468 = SelectFields.apply(frame=AWSGlueDataCatalog_node1712236954814, paths=["submission_types", "id", "course_id", "title", "updated_at", "workflow_state"], transformation_ctx="SelectFields_node1712237000468")

# Script generated for node Select Fields
SelectFields_node18796317 = SelectFields.apply(frame=AWSGlueDataCatalog_node187962810, paths=["assignment_id", "graded_at", "user_id", "submitted_at", "grader_id", "updated_at"], transformation_ctx="SelectFields_node18796317")


# Script generated for node SQL Query
SqlQuery0 = '''
select
submission_types,
id,
course_id,
title,
updated_at as assignment_date,
workflow_state

from myDataSource

where DATE(updated_at) between '2023-01-01' AND current_date() AND workflow_state like 'published' AND (submission_types = 'online_text_entry,online_upload' or submission_types= 'online_text_entry' or submission_types= 'online_upload')

'''
SQLQuery_node1712237165084 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":SelectFields_node1712237000468}, transformation_ctx = "SQLQuery_node1712237165084")

# Script generated for node Join
Join_node1711459726028 = Join.apply(frame1=SQLQuery_node1712237165084, frame2=SelectFields_node18796317, keys1=["id"], keys2=["assignment_id"], transformation_ctx="Join_node1711459726028")

# Segmento para agregar campo adicional con valor por defecto
spark_df = Join_node1711459726028.toDF();
dfNewColumn = spark_df.withColumn('id_data_origin', lit('2'));
Join_node1711459726028 = DynamicFrame.fromDF(dfNewColumn, glueContext, "convert");
# Segmento para agregar campo adicional con valor por defecto

# Script generated for node Change Schema
ChangeSchema_node171217625074750 = ApplyMapping.apply(frame=Join_node1711459726028, mappings=[("title", "string", "title", "string"), ("submitted_at", "timestamp", "date_delivery", "timestamp"), ("course_id", "long", "id_integration_subject", "long"), ("updated_at", "timestamp", "date_last_modification", "timestamp"),  ("grader_id", "long", "id_corrective_teacher", "long"), ("graded_at", "timestamp", "date_correction", "timestamp"),     ("user_id", "long", "id_student_internal", "long"), ("id_data_origin", "string", "id_data_origin", "long")], transformation_ctx="ChangeSchema_node171217625074750")

#######################Moodle ##############################################

# Script generated for node moodle_assing_submision
moodle_assing_submision_node1712169400725 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_assign_submission", transformation_ctx="moodle_assing_submision_node1712169400725")

# Script generated for node moodle_assign_grades
moodle_assign_grades_node1712169271348 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_assign_grades", transformation_ctx="moodle_assign_grades_node1712169271348")

# Script generated for node moodle_assign
moodle_assign_node1712169030245 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_assign", transformation_ctx="moodle_assign_node1712169030245")

# Script generated for node Select Fields
SelectFields_node1712169792100 = SelectFields.apply(frame=moodle_assing_submision_node1712169400725, paths=["assignment", "timemodified", "status"], transformation_ctx="SelectFields_node1712169792100")

# Script generated for node Select Fields
SelectFields_node1712169290119 = SelectFields.apply(frame=moodle_assign_grades_node1712169271348, paths=["assignment", "grader", "timemodified", "userid"], transformation_ctx="SelectFields_node1712169290119")

# Script generated for node Select Fields
SelectFields_node1712169241929 = SelectFields.apply(frame=moodle_assign_node1712169030245, paths=["name", "course", "id", "timemodified"], transformation_ctx="SelectFields_node1712169241929")

# Script generated for node Change Schema
ChangeSchema_node1712175371674 = ApplyMapping.apply(frame=SelectFields_node1712169792100, mappings=[("assignment", "long", "id_assignment", "long"), ("timemodified", "long", "date_delivery", "long"), ("status", "string", "status", "string")], transformation_ctx="ChangeSchema_node1712175371674")

# Script generated for node Change Schema
ChangeSchema_node1712175115467 = ApplyMapping.apply(frame=SelectFields_node1712169290119, mappings=[("assignment", "long", "assignment", "long"), ("timemodified", "long", "date_correction", "long"), ("grader", "long", "id_corrective_teacher", "long"), ("userid", "long", "internal_user_id", "long")], transformation_ctx="ChangeSchema_node1712175115467")

# Script generated for node Change Schema
ChangeSchema_node1712174947086 = ApplyMapping.apply(frame=SelectFields_node1712169241929, mappings=[("name", "string", "title", "string"), ("course", "long", "id_integration", "long"), ("id", "long", "id", "long"), ("timemodified", "long", "date_last_modification", "long")], transformation_ctx="ChangeSchema_node1712174947086")

# Script generated for node Join
Join_node1712175569105 = Join.apply(frame1=ChangeSchema_node1712174947086, frame2=ChangeSchema_node1712175115467, keys1=["id"], keys2=["assignment"], transformation_ctx="Join_node1712175569105")

# Script generated for node Join
Join_node1712175683738 = Join.apply(frame1=ChangeSchema_node1712175371674, frame2=Join_node1712175569105, keys1=["id_assignment"], keys2=["assignment"], transformation_ctx="Join_node1712175683738")

# Script generated for node Filter
Filter_node1712176097843 = Filter.apply(frame=Join_node1712175683738, f=lambda row: (bool(re.match("submitted", row["status"]))), transformation_ctx="Filter_node1712176097843")

# Segmento para agregar campo adicional con valor por defecto
spark_df = Filter_node1712176097843.toDF();
dfNewColumn = spark_df.withColumn('id_data_origin', lit('3'));
Filter_node1712176097843 = DynamicFrame.fromDF(dfNewColumn, glueContext, "convert");
# Segmento para agregar campo adicional con valor por defecto

# Script generated for node Change Schema
ChangeSchema_node1712176250747 = ApplyMapping.apply(frame=Filter_node1712176097843, mappings=[("date_delivery", "long", "date_delivery", "long"), ("title", "string", "title", "string"), ("id_integration", "long", "id_integration_subject", "long"), ("date_last_modification", "long", "date_last_modification", "long"), ("date_correction", "long", "date_correction", "long"), ("id_corrective_teacher", "long", "id_corrective_teacher", "long"), ("internal_user_id", "long", "internal_user_id", "long"),("id_data_origin", "string", "id_data_origin", "long")], transformation_ctx="ChangeSchema_node1712176250747")

# Script generated for node SQL Query
SqlQuery0 = '''
select 
title,
TO_TIMESTAMP(date_delivery) as date_delivery,
id_integration_subject,
TO_TIMESTAMP(date_last_modification) as date_last_modification,
id_corrective_teacher,
TO_TIMESTAMP(date_correction) as date_correction,
internal_user_id as id_student_internal,
id_data_origin
from myDataSource
'''
SQLQuery_node1712178146519 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":ChangeSchema_node1712176250747}, transformation_ctx = "SQLQuery_node1712178146519")


# Script generated for node Union
Union_node1711460953589 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": ChangeSchema_node171217625074750, "source2": SQLQuery_node1712178146519}, transformation_ctx = "Union_node1711460953589")

# Script generated for node Amazon Redshift
AmazonRedshift_node1712176561300 = glueContext.write_dynamic_frame.from_options(frame=Union_node1711460953589, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.activity", "connectionName": "Redshift Cluster Apify", "preactions": "DROP TABLE IF EXISTS public.activity; CREATE TABLE IF NOT EXISTS public.activity (id BIGINT IDENTITY(1,1), title VARCHAR, date_delivery TIMESTAMP, id_integration_subject VARCHAR, date_last_modification TIMESTAMP, id_corrective_teacher VARCHAR, date_correction TIMESTAMP, id_student_internal BIGINT, id_data_origin BIGINT);"}, transformation_ctx="AmazonRedshift_node1712176561300")


job.commit()

# Comentarios

# Connections => mysql Moodle test,  Redshift cluster apify, redshift micampues

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED