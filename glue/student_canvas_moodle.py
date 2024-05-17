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

# Script generated for node moodle_user
moodle_user_node1712150771226 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_user", transformation_ctx="moodle_user_node1712150771226")

# Script generated for node moodle_role
moodle_role_node1712150470987 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_role", transformation_ctx="moodle_role_node1712150470987")

# Script generated for node canvas_role
canvas_role_node1712151752162 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_role_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/", 
    transformation_ctx="canvas_role_node1712151752162")

# Script generated for node canvas_users
canvas_users_node1712156307483 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_user_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/", 
    transformation_ctx="canvas_users_node1712156307483")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1712156452100 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_pseudonym_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/", 
    transformation_ctx="AWSGlueDataCatalog_node1712156452100")

# Script generated for node canvas_enrollment
canvas_enrollment_node1712154733281 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_enrollment_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/", 
    transformation_ctx="canvas_enrollment_node1712154733281")

# Script generated for node moodle_role_assigments
moodle_role_assigments_node1712150595198 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_role_assignments", transformation_ctx="moodle_role_assigments_node1712150595198")

# Script generated for node Select Fields
SelectFields_node1712150824794 = SelectFields.apply(frame=moodle_user_node1712150771226, paths=["id", "firstname", "idnumber", "lastname"], transformation_ctx="SelectFields_node1712150824794")

# Script generated for node Select Fields
SelectFields_node1712150528515 = SelectFields.apply(frame=moodle_role_node1712150470987, paths=["id", "shortname"], transformation_ctx="SelectFields_node1712150528515")

# Script generated for node Select Fields
SelectFields_node1712151798083 = SelectFields.apply(frame=canvas_role_node1712151752162, paths=["name", "id"], transformation_ctx="SelectFields_node1712151798083")

# Script generated for node Select Fields
SelectFields_node1712156365709 = SelectFields.apply(frame=canvas_users_node1712156307483, paths=["name", "id"], transformation_ctx="SelectFields_node1712156365709")

# Script generated for node Select Fields
SelectFields_node1712156470664 = SelectFields.apply(frame=AWSGlueDataCatalog_node1712156452100, paths=["sis_user_id", "user_id", "workflow_state"], transformation_ctx="SelectFields_node1712156470664")

# Script generated for node Select Fields
SelectFields_node1712154761247 = SelectFields.apply(frame=canvas_enrollment_node1712154733281, paths=["role_id", "user_id"], transformation_ctx="SelectFields_node1712154761247")

# Script generated for node Select Fields
SelectFields_node1712150661146 = SelectFields.apply(frame=moodle_role_assigments_node1712150595198, paths=["roleid", "userid"], transformation_ctx="SelectFields_node1712150661146")

# Script generated for node SQL Query
SqlQuery1 = '''
select 
id as internal_user_id,
idnumber,
CONCAT(firstname, ' ' ,lastname) AS fullname
from myDataSource
'''
SQLQuery_node1712150877547 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":SelectFields_node1712150824794}, transformation_ctx = "SQLQuery_node1712150877547")

# Script generated for node Filter
Filter_node1712150539542 = Filter.apply(frame=SelectFields_node1712150528515, f=lambda row: (bool(re.match("student", row["shortname"]))), transformation_ctx="Filter_node1712150539542")

# Script generated for node Filter
Filter_node1712156790585 = Filter.apply(frame=SelectFields_node1712151798083, f=lambda row: (bool(re.match("StudentEnrollment", row["name"]))), transformation_ctx="Filter_node1712156790585")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where sis_user_id IS NOT NULL and workflow_state = 'active'
'''
SQLQuery_node1712156636553 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":SelectFields_node1712156470664}, transformation_ctx = "SQLQuery_node1712156636553")

# Script generated for node Join
Join_node1712156866620 = Join.apply(frame1=Filter_node1712156790585, frame2=SelectFields_node1712154761247, keys1=["id"], keys2=["role_id"], transformation_ctx="Join_node1712156866620")

# Script generated for node Join
Join_node1712150698352 = Join.apply(frame1=SelectFields_node1712150661146, frame2=Filter_node1712150539542, keys1=["roleid"], keys2=["id"], transformation_ctx="Join_node1712150698352")

# Script generated for node Join
Join_node1712150935850 = Join.apply(frame1=Join_node1712150698352, frame2=SQLQuery_node1712150877547, keys1=["userid"], keys2=["internal_user_id"], transformation_ctx="Join_node1712150935850")

# Segmento para agregar campo adicional con valor por defecto
spark_df = Join_node1712150935850.toDF();
dfNewColumn = spark_df.withColumn('id_data_origin', lit('1'));
Join_node1712150935850 = DynamicFrame.fromDF(dfNewColumn, glueContext, "convert");
# Segmento para agregar campo adicional con valor por defecto

# Script generated for node Join
Join_node1712156694037 = Join.apply(frame1=SelectFields_node1712156365709, frame2=SQLQuery_node1712156636553, keys1=["id"], keys2=["user_id"], transformation_ctx="Join_node1712156694037")

# Script generated for node Select Fields
SelectFields_node1712157094677 = SelectFields.apply(frame=Join_node1712156866620, paths=["role_id", "user_id"], transformation_ctx="SelectFields_node1712157094677")

# Script generated for node Change Schema
ChangeSchema_node1712151004616 = ApplyMapping.apply(frame=Join_node1712150935850, mappings=[("idnumber", "string", "id_integration_proeduca_user", "string"),  ("id_data_origin", "string", "id_data_origin", "long"),  ("fullname", "string", "fullname", "string"), ("internal_user_id", "long", "id_internal_user", "long")], transformation_ctx="ChangeSchema_node1712151004616")

# Script generated for node Select Fields
SelectFields_node1712156734780 = SelectFields.apply(frame=Join_node1712156694037, paths=["sis_user_id", "user_id", "name"], transformation_ctx="SelectFields_node1712156734780")

# Script generated for node Join
Join_node1712157156158 = Join.apply(frame1=SelectFields_node1712156734780, frame2=SelectFields_node1712157094677, keys1=["user_id"], keys2=["user_id"], transformation_ctx="Join_node1712157156158")

# Segmento para agregar campo adicional con valor por defecto
spark_df = Join_node1712157156158.toDF();
dfNewColumn = spark_df.withColumn('id_data_origin', lit('2'));
Join_node1712157156158 = DynamicFrame.fromDF(dfNewColumn, glueContext, "convert");
# Segmento para agregar campo adicional con valor por defecto

# Script generated for node Change Schema
ChangeSchema_node1712157174274 = ApplyMapping.apply(frame=Join_node1712157156158, mappings=[("sis_user_id", "string", "id_integration_proeduca_user", "string"),  ("id_data_origin", "string", "id_data_origin", "long"), ("name", "string", "fullname", "string"), ("user_id", "long", "id_internal_user", "long")], transformation_ctx="ChangeSchema_node1712157174274")

DropDuplicates_node1712159756683 =  DynamicFrame.fromDF(ChangeSchema_node1712157174274.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1712159756683")



# Script generated for node Union
Union_node1712157299843 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": ChangeSchema_node1712151004616, "source2": DropDuplicates_node1712159756683}, transformation_ctx = "Union_node1712157299843")

drop_duplicates_node =  DynamicFrame.fromDF(Union_node1712157299843.toDF().dropDuplicates(), glueContext, "drop_duplicates_node")

# Script generated for node Amazon Redshift
AmazonRedshift_node1712151213278 = glueContext.write_dynamic_frame.from_options(frame=drop_duplicates_node, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.student", "connectionName": "Redshift Cluster Apify", "preactions": "CREATE TABLE IF NOT EXISTS public.student (id_integration_proeduca_user VARCHAR, id_data_origin BIGINT, fullname VARCHAR, id_internal_user BIGINT);"}, transformation_ctx="AmazonRedshift_node1712151213278")

job.commit()

# Comentarios

# Connections => mysql Moodle test,  Redshift cluster apify, redshift micampues

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED