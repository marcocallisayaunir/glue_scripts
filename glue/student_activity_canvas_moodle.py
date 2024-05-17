import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node redshift_activity
redshift_activity_node1712751418472 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.activity", "connectionName": "Redshift Cluster Apify"}, transformation_ctx="redshift_activity_node1712751418472")

# Script generated for node Select Fields
SelectFields_node1712751477720 = SelectFields.apply(frame=redshift_activity_node1712751418472, paths=["id", "id_student_internal"], transformation_ctx="SelectFields_node1712751477720")

# Script generated for node Change Schema
ChangeSchema_node1712751581942 = ApplyMapping.apply(frame=SelectFields_node1712751477720, mappings=[("id", "long", "id_activity", "long"), ("id_student_internal", "long", "id_user_internal", "string")], transformation_ctx="ChangeSchema_node1712751581942")

# Script generated for node Amazon Redshift
AmazonRedshift_node1712751696519 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1712751581942, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.student_activity", "connectionName": "Redshift Cluster Apify", "preactions": "DROP TABLE IF EXISTS public.student_activity; CREATE TABLE IF NOT EXISTS public.student_activity (id BIGINT IDENTITY(1,1), id_activity BIGINT, id_user_internal VARCHAR);"}, transformation_ctx="AmazonRedshift_node1712751696519")

job.commit()

# Comentarios

# Connections => Redshift cluster apify