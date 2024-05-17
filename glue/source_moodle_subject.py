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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
moodle_instances = [
    {"nombre": "otavalo", "fuente": "7"},
    {"nombre": "tu_campus", "fuente": "3"},
    {"nombre": "unipro", "fuente": "6"},
    {"nombre": "educacion", "fuente": "1"}
]
for instance in moodle_instances:
    
    # Script generated for node source_moodle_course
    source_moodle_course_node = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name=instance['nombre'] + "_mdl_course", transformation_ctx="source_moodle_course_node")
    
    # Segmento para agregar campo adicional con valor por defecto
    spark_df2 = source_moodle_course_node.toDF();
    dfNewColumn2 = spark_df2.withColumn('id_data_origin', lit(instance['fuente']));
    source_moodle_course_node = DynamicFrame.fromDF(dfNewColumn2, glueContext, "convert");
    
    # Script generated for node select_fields_source
    select_fields_source_node = SelectFields.apply(frame=source_moodle_course_node, paths=["idnumber", "startdate", "id", "timecreated", "fullname", "category", "timemodified", "id_data_origin"], transformation_ctx="select_fields_source_node")
    
    # Script generated for node filter_sis_id_moodle
    filter_sis_id_moodle_node = Filter.apply(frame=select_fields_source_node, f=lambda row: (bool(re.match("^([^|]+)\|([^|]+)\|([^_]+)__([^|]+)", row["idnumber"])) and row["timecreated"] >= 1672531200), transformation_ctx="filter_sis_id_moodle_node")
    
    # Verificar si no hay datos que cumplan con el filtro
    if filter_sis_id_moodle_node.count() == 0:
        print(f"No hay datos que cumplan con el filtro en la instancia {instance['nombre']}. Saltando al siguiente elemento del array.")
        continue
    
    # Script generated for node query_sql_filter
    SqlQuery1937 = '''
    select
    SUBSTRING_INDEX(SUBSTRING_INDEX(idnumber, '|', 2), '|', -1) AS id_integration_subject,
    SUBSTRING_INDEX(idnumber, '|', 1) AS periods,
    SUBSTRING_INDEX(idnumber, '|', -1) AS id_segment_integration,
    id as id_internal_subject,
    fullname as description,
    category as id_internal_study,
    id_data_origin,
    TO_TIMESTAMP(timecreated) AS created_at
    from myDataSource
    '''
    query_sql_filter_node = sparkSqlQuery(glueContext, query = SqlQuery1937, mapping = {"myDataSource":filter_sis_id_moodle_node}, transformation_ctx = "query_sql_filter_node")
    
    # Script generated for node change_schema
    change_schema_node = ApplyMapping.apply(frame=query_sql_filter_node, mappings=[("id_integration_subject", "string", "id_integration_subject", "string"), ("periods", "string", "periods", "string"), ("id_segment_integration", "string", "id_segment_integration", "string"), ("id_internal_subject", "long", "id_internal_subject", "long"), ("description", "string", "description", "string"), ("id_internal_study", "long", "id_internal_study", "int"), ("id_data_origin", "string", "id_data_origin", "int"), ("created_at", "timestamp", "created_at", "timestamp")], transformation_ctx="change_schema_node")
    
    # Script generated for node tarjet_source_moodle
    tarjet_source_moodle_node1715523961675 = glueContext.write_dynamic_frame.from_options(frame=change_schema_node, connection_type="redshift", 
        connection_options={
            "postactions": "BEGIN; DELETE FROM source.subjects_temp_1u3gbe USING source.subjects WHERE source.subjects_temp_1u3gbe.id_data_origin = source.subjects.id_data_origin; INSERT INTO source.subjects (id_integration_subject, periods, id_segment_integration, id_internal_subject, description, id_internal_study, id_data_origin, created_at) SELECT id_integration_subject, periods, id_segment_integration, id_internal_subject, description, id_internal_study,  id_data_origin, created_at FROM source.subjects_temp_1u3gbe; DROP TABLE source.subjects_temp_1u3gbe; END;",
            "redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", 
            "useConnectionProperties": "true", 
            "dbtable": "source.subjects_temp_1u3gbe", 
            "connectionName": "Redshift moodle sources connection"
            }, 
        transformation_ctx="tarjet_source_moodle_node1715523961675"
        )

job.commit()


# Comentarios

# Connections => mysql education,  Redshift moodle source connection

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED