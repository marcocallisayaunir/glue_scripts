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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node moodle_assing_submision
moodle_assing_submision_node1712169400725 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodletest_lms_moodle_des_mdl_assign_submission", transformation_ctx="moodle_assing_submision_node1712169400725")

# Script generated for node moodle_assign_grades
moodle_assign_grades_node1712169271348 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodletest_lms_moodle_des_mdl_assign_grades", transformation_ctx="moodle_assign_grades_node1712169271348")

# Script generated for node moodle_assign
moodle_assign_node1712169030245 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodletest_lms_moodle_des_mdl_assign", transformation_ctx="moodle_assign_node1712169030245")

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

# Script generated for node Change Schema
ChangeSchema_node1712176250747 = ApplyMapping.apply(frame=Filter_node1712176097843, mappings=[("date_delivery", "long", "date_delivery", "long"), ("title", "string", "title", "string"), ("id_integration", "long", "id_integration_subject", "long"), ("date_last_modification", "long", "date_last_modification", "long"), ("date_correction", "long", "date_correction", "long"), ("id_corrective_teacher", "long", "id_corrective_teacher", "long"), ("internal_user_id", "long", "internal_user_id", "long")], transformation_ctx="ChangeSchema_node1712176250747")

# Script generated for node SQL Query
SqlQuery0 = '''
select 
title,
TO_TIMESTAMP(date_delivery) as date_delivery,
id_integration_subject,
TO_TIMESTAMP(date_last_modification) as date_last_modification,
id_corrective_teacher,
TO_TIMESTAMP(date_correction) as date_correction,
internal_user_id
from myDataSource
'''
SQLQuery_node1712178146519 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":ChangeSchema_node1712176250747}, transformation_ctx = "SQLQuery_node1712178146519")

# Script generated for node Amazon Redshift
AmazonRedshift_node1712176561300 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1712178146519, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.activity", "connectionName": "Redshift Cluster Apify", "preactions": "DROP TABLE IF EXISTS public.activity; CREATE TABLE IF NOT EXISTS public.activity (title VARCHAR, date_delivery TIMESTAMP, id_integration_subject VARCHAR, date_last_modification TIMESTAMP, id_corrective_teacher VARCHAR, date_correction TIMESTAMP, internal_user_id VARCHAR);"}, transformation_ctx="AmazonRedshift_node1712176561300")

job.commit()


# Comentarios

# Connections => mysql Moodle test y Redshift cluster apify

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED