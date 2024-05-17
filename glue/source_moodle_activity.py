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
    source_activity_node = glueContext.create_dynamic_frame.from_catalog(
        database="apify", 
        table_name=instance['nombre'] +"_mdl_grade_items", 
        transformation_ctx="source_activity_node")
        
    # Segmento para agregar campo adicional con valor por defecto
    spark_df2 = source_activity_node.toDF();
    dfNewColumn2 = spark_df2.withColumn('id_data_origin', lit(instance['fuente']));
    source_activity_node = DynamicFrame.fromDF(dfNewColumn2, glueContext, "convert");
    
    # Script generated for node Select Fields
    select_fields_categories = SelectFields.apply(frame=source_activity_node, paths=["id", "itemtype", "itemmodule", "iteminstance", "courseid", "itemname", "gradetype" , "timecreated" , "id_data_origin"], transformation_ctx="select_fields_categories")
    
    # Script generated for node Filter
    filter_idnumber_categories = Filter.apply(frame=select_fields_categories, f=lambda row: (row["timecreated"] >= 1672531200), transformation_ctx="filter_idnumber_categories")
    
    # Verificar si no hay datos que cumplan con el filtro
    if filter_idnumber_categories.count() == 0:
        print(f"No hay datos que cumplan con el filtro en la instancia {instance['nombre']}. Saltando al siguiente elemento del array.")
        continue
    
    SqlQuery0 = '''
    SELECT 
        id,
        itemtype,
        id_data_origin,
        itemmodule,
        iteminstance,
        courseid,
        itemname,
        'gradeable' gradetype 
        from moodle
        where itemtype = 'mod' and itemmodule IN ('assign', 'quiz')
    '''
    sql_query_sis_id_moodle = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"moodle":filter_idnumber_categories}, transformation_ctx = "sql_query_sis_id_moodle")
    
    # Script generated for node Change Schema
    change_schema_nodemoodle = ApplyMapping.apply(frame=sql_query_sis_id_moodle, mappings=[("id", "long", "id_internal_activity", "long"), ("iteminstance", "long", "id_module_internal", "long"), ("itemmodule", "string", "type_module", "string"), ("courseid", "long", "id_internal_subject", "long") , ("itemname", "string", "title", "string"), ("gradetype", "string", "grade_type_activity", "string"), ("id_data_origin", "string", "id_data_origin", "long")], transformation_ctx="change_schema_nodemoodle")
    
    # Script generated for node tarjet_source_moodle
    tarjet_source_moodle_node1715523961675 = glueContext.write_dynamic_frame.from_options(frame=change_schema_nodemoodle,connection_type="redshift", 
        connection_options={
            "postactions": "BEGIN; DELETE FROM source.activities_temp_1u3gbe USING source.activities WHERE source.activities_temp_1u3gbe.id_data_origin = source.activities.id_data_origin; INSERT INTO source.activities (id_internal_subject, id_internal_activity, title, type_module, id_module_internal, grade_type_activity, id_data_origin ) SELECT id_internal_subject, id_internal_activity, title, type_module, id_module_internal, grade_type_activity, id_data_origin FROM source.activities_temp_1u3gbe; DROP TABLE source.activities_temp_1u3gbe; END;",
            "redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", 
            "useConnectionProperties": "true", 
            "dbtable": "source.activities_temp_1u3gbe", 
            "connectionName": "Redshift moodle sources connection"
            }, 
        transformation_ctx="tarjet_source_moodle_node1715523961675"
        )

job.commit()

# Comentarios

# Connections => mysql education,  Redshift moodle source connection

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED
