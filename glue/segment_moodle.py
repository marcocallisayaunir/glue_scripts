import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import re
import gs_uuid
from pyspark.sql import functions as SqlFuncs

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

# Leer los datos del nodo courses_moodle_node
courses_moodle_node = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_course", transformation_ctx="courses_moodle_node")

# Seleccionar y filtrar los datos necesarios
select_fields_node = SelectFields.apply(frame=courses_moodle_node, paths=["idnumber", "timecreated", "timemodified"], transformation_ctx="select_fields_node")

filter_idnumber_timemodified_node = Filter.apply(frame=select_fields_node, f=lambda row: (bool(re.match("^(\d+-\d+-\d+-\d+)\|(\d+)\|(\d+)__(\d+)$", row["idnumber"])) and row["timecreated"] >= 1672531200), transformation_ctx="filter_idnumber_timemodified_node")

# Consultar la tabla temporal
SqlQuery0 = '''
select
SUBSTRING_INDEX(idnumber, '|', -1) AS id_segment_integration
from DataSourceMoodle
'''
query_segment_course_node = sparkSqlQuery(glueContext, query=SqlQuery0, mapping={"DataSourceMoodle": filter_idnumber_timemodified_node}, transformation_ctx="query_segment_course_node")

# Cambiar el esquema de la tabla
change_schema_node = ApplyMapping.apply(frame=query_segment_course_node, mappings=[("id_segment_integration", "string", "id_segment_integration", "string"), ("id", "string", "id", "string")], transformation_ctx="change_schema_node")

# Eliminar duplicados de los datos
drop_duplicates_node =  DynamicFrame.fromDF(change_schema_node.toDF().dropDuplicates(), glueContext, "drop_duplicates_node")

# canvas

# Script generated for node canvas_course
canvas_course_node1712003585579 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="micampus_micampus_public_course_dim",  redshift_tmp_dir="s3://g-redshift-temp/temporal/", transformation_ctx="canvas_course_node1712003585579")

# Script generated for node select_fields_course_canvas
select_fields_course_canvas_node1712003877237 = SelectFields.apply(frame=canvas_course_node1712003585579, paths=["sis_source_id", "created_at", "workflow_state"], transformation_ctx="select_fields_course_canvas_node1712003877237")

# Script generated for node filter_course_canvas
filter_course_canvas_node1712004032121 = Filter.apply(frame=select_fields_course_canvas_node1712003877237, f=lambda row: (bool(re.match("^(\d+-\d+-\d+-\d+)\|(\d+)\|(\d+)__(\d+)$", row["sis_source_id"]))), transformation_ctx="filter_course_canvas_node1712004032121")

# Script generated for node query_course_canvas
SqlQuery2 = '''
select
SUBSTRING_INDEX(sis_source_id, '|', -1) AS id_segment_integration
from myDataSource where created_at >= '2023-01-01' and workflow_state != 'deleted'
'''
query_course_canvas_node1712004217996 = sparkSqlQuery(glueContext, query = SqlQuery2, mapping = {"myDataSource":filter_course_canvas_node1712004032121}, transformation_ctx = "query_course_canvas_node1712004217996")

drop_duplicates_node_canvas =  DynamicFrame.fromDF(query_course_canvas_node1712004217996.toDF().dropDuplicates(), glueContext, "drop_duplicates_node_canvas")

AmazonRedshift_node17127638764100 = glueContext.write_dynamic_frame.from_options(
    frame=drop_duplicates_node_canvas,
    connection_type="redshift",
    connection_options={
        "postactions": "BEGIN; INSERT INTO segment_temp_hr9q3y_staging SELECT DISTINCT LEFT(id_segment_integration, POSITION('__' IN id_segment_integration) - 1) AS id_segment_integration FROM public.segment_temp_hr9q3y_staging; INSERT INTO public.segment (id_segment_integration) SELECT id_segment_integration FROM segment_temp_hr9q3y_staging WHERE id_segment_integration NOT IN (SELECT id_segment_integration FROM public.segment); DROP TABLE IF EXISTS segment_temp_hr9q3y_staging; COMMIT;",
        "redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.segment_temp_hr9q3y_staging",
        "connectionName": "Redshift Cluster Apify"
    },
    transformation_ctx="AmazonRedshift_node17127638764100"
)


job.commit()

# Comentarios

# Connections => mysql Moodle test y Redshift cluster apify

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED