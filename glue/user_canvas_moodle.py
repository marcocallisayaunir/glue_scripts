import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import lit

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

    
   ############### MOODLE ##########################################
    # Script generated for node moodle_users
moodle_users_node1710952309926 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_user", transformation_ctx="moodle_users_node1710952309926")

    # Script generated for node Select Fields
SelectFields_node1710952377091 = SelectFields.apply(frame=moodle_users_node1710952309926, paths=["idnumber", "username", "firstname", "lastname", "id"], transformation_ctx="SelectFields_node1710952377091")
    
    # Script generated for node SQL Query
SqlQuery18760 = '''
    select 
    idnumber,
    username,
    id
    from myDataSource
    
    '''
SQLQuery_node17114596707490 = sparkSqlQuery(glueContext, query = SqlQuery18760, mapping = {"myDataSource":SelectFields_node1710952377091}, transformation_ctx = "SQLQuery_node1711459670749")
    
    # Script generated for node Change Schema
ChangeSchema_node1710952459869 = ApplyMapping.apply(frame=SQLQuery_node17114596707490, mappings=[("idnumber", "string", "id_integration_proeduca_user", "string"), ("username", "string", "user_login", "string"), ("id", "long", "id_internal_user", "long")], transformation_ctx="ChangeSchema_node1710952459869")
  

############### CANVAS ##########################################

# Script generated for node canvas_pseudonym
canvas_pseudonym_node1711459142797 = glueContext.create_dynamic_frame.from_catalog(
    database="apify", 
    table_name="micampus_micampus_public_pseudonym_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/", 
    transformation_ctx="canvas_pseudonym_node1711459142797")

# Script generated for node canvas_users
canvas_users_node1711459109032 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_user_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/", 
    transformation_ctx="canvas_users_node1711459109032")


# Script generated for node Select Fields
SelectFields_node1711459480459 = SelectFields.apply(frame=canvas_pseudonym_node1711459142797, paths=["user_id", "sis_user_id", "unique_name", "workflow_state"], transformation_ctx="SelectFields_node1711459480459")

# Script generated for node Select Fields
SelectFields_node1711459408371 = SelectFields.apply(frame=canvas_users_node1711459109032, paths=["id", "name"], transformation_ctx="SelectFields_node1711459408371")


# Script generated for node SQL Query
SqlQuery1876 = '''
select * from myDataSource
where sis_user_id IS NOT NULL and workflow_state = 'active'
'''
SQLQuery_node1711459670749 = sparkSqlQuery(glueContext, query = SqlQuery1876, mapping = {"myDataSource":SelectFields_node1711459480459}, transformation_ctx = "SQLQuery_node1711459670749")

# Script generated for node Join
Join_node1711459726028 = Join.apply(frame1=SelectFields_node1711459408371, frame2=SQLQuery_node1711459670749, keys1=["id"], keys2=["user_id"], transformation_ctx="Join_node1711459726028")


# Script generated for node Change Schema
ChangeSchema_node1711459783998 = ApplyMapping.apply(frame=Join_node1711459726028, mappings=[("sis_user_id", "string", "id_integration_proeduca_user", "string"), ("unique_name", "string", "user_login", "string"),  ("user_id", "long", "id_internal_user", "long")], transformation_ctx="ChangeSchema_node1711459783998")

############## UNION INSNTANCES #############################################

# Script generated for node Union
Union_node1711460953589 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": ChangeSchema_node1711459783998, "source2": ChangeSchema_node1710952459869}, transformation_ctx = "Union_node1711460953589")

drop_duplicates_node =  DynamicFrame.fromDF(Union_node1711460953589.toDF().dropDuplicates(), glueContext, "drop_duplicates_node")

# Script generated for node Amazon Redshift
AmazonRedshift_node1710952543229 = glueContext.write_dynamic_frame.from_options(frame=drop_duplicates_node, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.user", "connectionName": "Redshift Cluster Apify", "preactions": "CREATE TABLE IF NOT EXISTS public.user (id_integration_proeduca_user VARCHAR, user_login VARCHAR, id_internal_user BIGINT);"}, transformation_ctx="AmazonRedshift_node1710952543229")

job.commit()

# Comentarios

# Connections => mysql Moodle test,  Redshift cluster apify, redshift micampues

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED
