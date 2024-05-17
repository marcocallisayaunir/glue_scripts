import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import lit

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
moodle_course_node1712838324981 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="moodletest_lms_moodle_des_mdl_course",
    transformation_ctx="moodle_course_node1712838324981")

# Script generated for node canvas_course
canvas_course_node1712838470505 = glueContext.create_dynamic_frame.from_catalog(
    database="apify",
    table_name="micampus_micampus_public_course_dim",
    redshift_tmp_dir="s3://g-redshift-temp/temporal/",
    transformation_ctx="canvas_course_node1712838470505")

# Script generated for node Select Fields
SelectFields_node1712838960886 = SelectFields.apply(frame=moodle_course_node1712838324981, paths=["id", "idnumber", "visible", "shortname", "fullname"], transformation_ctx="SelectFields_node1712838960886")

# Segmento para agregar campo adicional con valor por defecto
spark_df = SelectFields_node1712838960886.toDF();
dfNewColumn = spark_df.withColumn('id_data_origin', lit('moodle'));
SelectFields_node1712838960886 = DynamicFrame.fromDF(dfNewColumn, glueContext, "convert");
# Segmento para agregar campo adicional con valor por defecto

# Script generated for node Select Fields
SelectFields_node1712839184871 = SelectFields.apply(frame=canvas_course_node1712838470505, paths=["sis_source_id", "canvas_id", "publicly_visible", "name", "code"], transformation_ctx="SelectFields_node1712839184871")

# Segmento para agregar campo adicional con valor por defecto
spark_df = SelectFields_node1712839184871.toDF();
dfNewColumn = spark_df.withColumn('id_data_origin', lit('micampus'));
SelectFields_node1712839184871 = DynamicFrame.fromDF(dfNewColumn, glueContext, "convert");
# Segmento para agregar campo adicional con valor por defecto


# Script generated for node Change Schema
ChangeSchema_node1712839284904 = ApplyMapping.apply(frame=SelectFields_node1712838960886, mappings=[("idnumber", "string", "sis_id", "string"),("id", "long", "id", "long"),  ("visible", "boolean", "visible", "boolean"), ("shortname", "string", "code", "string"), ("fullname", "string", "name", "string"), ("id_data_origin", "string", "id_data_origin", "string")], transformation_ctx="ChangeSchema_node1712839284904")

# Script generated for node Change Schema
ChangeSchema_node1712839358838 = ApplyMapping.apply(frame=SelectFields_node1712839184871, mappings=[("sis_source_id", "string", "sis_id", "string"), ("canvas_id", "long", "id", "long"), ("publicly_visible", "boolean", "visible", "boolean"),  ("code", "string", "code", "string"),   ("name", "string", "name", "string"), ("id_data_origin", "string", "id_data_origin",  "string")], transformation_ctx="ChangeSchema_node1712839358838")

# Script generated for node Union
Union_node1711049581168 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": ChangeSchema_node1712839284904, "source2": ChangeSchema_node1712839358838}, transformation_ctx = "Union_node1711049581168")



# Script generated for node Amazon Redshift
AmazonRedshift_node1711049602399 = glueContext.write_dynamic_frame.from_options(
    frame=Union_node1711049581168, connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.course_demo", "connectionName": "Redshift Cluster Apify", "preactions": "DROP TABLE IF EXISTS public.course_demo; CREATE TABLE IF NOT EXISTS public.course_demo (sis_id VARCHAR, id BIGINT, visible boolean, code VARCHAR, name VARCHAR, id_data_origin VARCHAR);"}, transformation_ctx="AmazonRedshift_node1711049602399")

job.commit()

# Comentarios

# Connections => mysql Moodle test y Redshift cluster apify

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED