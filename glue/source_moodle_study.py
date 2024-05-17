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
    {"nombre": "educacion", "fuente": "1"},
    {"nombre": "humanium", "fuente": "2"}
]
for instance in moodle_instances:
    # Script generated for node AWS Glue Data Catalog
    source_course_categories_node = glueContext.create_dynamic_frame.from_catalog(
        database="apify", 
        table_name=instance['nombre'] +"_mdl_course_categories", 
        transformation_ctx="source_course_categories_node")
        
    # Segmento para agregar campo adicional con valor por defecto
    spark_df2 = source_course_categories_node.toDF();
    dfNewColumn2 = spark_df2.withColumn('id_data_origin', lit(instance['fuente']));
    source_course_categories_node = DynamicFrame.fromDF(dfNewColumn2, glueContext, "convert");
    
    # Script generated for node Select Fields
    felect_fields_categories = SelectFields.apply(frame=source_course_categories_node, paths=["idnumber", "name", "timemodified", "id", "id_data_origin"], transformation_ctx="felect_fields_categories")
    
    # Script generated for node Filter
    filter_idnumber_categories = Filter.apply(frame=felect_fields_categories, f=lambda row: (bool(re.match("^([^|]+)\|([^_]+)__([^|]+)\|([^|]+)", row["idnumber"])) and row["timemodified"] >= 1672531200), transformation_ctx="filter_idnumber_categories")
    
    # Verificar si no hay datos que cumplan con el filtro
    if filter_idnumber_categories.count() == 0:
        print(f"No hay datos que cumplan con el filtro en la instancia {instance['nombre']}. Saltando al siguiente elemento del array.")
        continue
    
    SqlQuery0 = '''
    SELECT 
        SUBSTRING_INDEX(idnumber, '|', 1) AS idnumber,
        name,
        id,
        id_data_origin,
        TO_TIMESTAMP(CURRENT_DATE) AS created_at
        from moodle
    '''
    sql_query_sis_id_moodle = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"moodle":filter_idnumber_categories}, transformation_ctx = "sql_query_sis_id_moodle")
    
    # Script generated for node Change Schema
    change_schema_nodemoodle = ApplyMapping.apply(frame=sql_query_sis_id_moodle, mappings=[("idnumber", "string", "id_integration_study", "string"), ("name", "string", "description", "string"), ("id", "long", "id_internal_study", "long"), ("id_data_origin", "string", "id_data_origin", "long"), ("created_at", "timestamp", "created_at", "timestamp")], transformation_ctx="change_schema_nodemoodle")
    
    # Script generated for node tarjet_source_moodle
    tarjet_source_moodle_node1715523961675 = glueContext.write_dynamic_frame.from_options(frame=change_schema_nodemoodle,connection_type="redshift", 
        connection_options={
            "postactions": "BEGIN; DELETE FROM source.studys_temp_1u3gbe USING source.studys WHERE source.studys_temp_1u3gbe.id_data_origin = source.studys.id_data_origin; INSERT INTO source.studys (id_integration_study, description, id_internal_study, id_data_origin, created_at) SELECT id_integration_study, description, id_internal_study, id_data_origin, created_at FROM source.studys_temp_1u3gbe; DROP TABLE source.studys_temp_1u3gbe; END;",
            "redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", 
            "useConnectionProperties": "true", 
            "dbtable": "source.studys_temp_1u3gbe", 
            "connectionName": "Redshift moodle sources connection"
            }, 
        transformation_ctx="tarjet_source_moodle_node1715523961675"
        )

job.commit()

# Comentarios

# Connections => mysql education,  Redshift moodle source connection

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED