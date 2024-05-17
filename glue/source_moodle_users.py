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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

moodle_instances = [
    {"nombre": "educacion", "fuente": "1"},
    {"nombre": "humanium", "fuente": "2"},
    {"nombre": "tu_campus", "fuente": "3"},
    {"nombre": "decroly", "fuente": "4"},
    {"nombre": "gif", "fuente": "5"},
    {"nombre": "octavalo", "fuente": "7"},
    {"nombre": "unipro", "fuente": "6"},
    {"nombre": "cunimad", "fuente": "15"}
    ]
for instance in moodle_instances:

    # Script generated for node source_users
    source_users_node1715523652320 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name=instance['nombre'] + "_mdl_user", transformation_ctx="source_users_node1715523652320")
    
    # Segmento para agregar campo adicional con valor por defecto
    spark_df2 = source_users_node1715523652320.toDF();
    dfNewColumn2 = spark_df2.withColumn('id_data_origin', lit(instance['fuente']));
    source_users_node1715523652320 = DynamicFrame.fromDF(dfNewColumn2, glueContext, "convert");
    
    # Script generated for node select_fields
    select_fields_node1715523737333 = SelectFields.apply(frame=source_users_node1715523652320, paths=["id", "idnumber", "lastname", "firstname", "username", "timecreated", "deleted", "id_data_origin"], transformation_ctx="select_fields_node1715523737333")
    
    # Script generated for node SQL Query
    SqlQuery1522 = '''
    select 
        idnumber AS id_integration_proeduca,
        id AS id_internal_user,
        username AS user_login,
        concat(firstname, ' ', lastname) as full_name,
        id_data_origin,
        TO_TIMESTAMP(timecreated) AS created_at
    from myDataSource where deleted = 0
    
    '''
    SQLQuery_node1715523807513 = sparkSqlQuery(glueContext, query = SqlQuery1522, mapping = {"myDataSource":select_fields_node1715523737333}, transformation_ctx = "SQLQuery_node1715523807513")
    
    # Script generated for node change_schema_users
    change_schema_users_node1715523947032 = ApplyMapping.apply(frame=SQLQuery_node1715523807513, mappings=[("id_integration_proeduca", "string", "id_integration_proeduca", "string"), ("id_internal_user", "long", "id_internal_user", "long"), ("user_login", "string", "user_login", "string"), ("full_name", "string", "full_name", "string"), ("id_data_origin", "string", "id_data_origin", "long"), ("created_at", "timestamp", "created_at", "timestamp")], transformation_ctx="change_schema_users_node1715523947032")
    
    # Script generated for node tarjet_source_moodle
    tarjet_source_moodle_node1715523961675 = glueContext.write_dynamic_frame.from_options(frame=change_schema_users_node1715523947032,connection_type="redshift", 
        connection_options={
            "postactions": "BEGIN; DELETE FROM source.users_temp_1u3gbe USING source.users WHERE source.users_temp_1u3gbe.id_data_origin = source.users.id_data_origin; INSERT INTO source.users (id_integration_proeduca, id_internal_user, user_login, full_name, id_data_origin, created_at) SELECT id_integration_proeduca, id_internal_user, user_login, full_name, id_data_origin, created_at FROM source.users_temp_1u3gbe; DROP TABLE source.users_temp_1u3gbe; END;",
            "redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", 
            "useConnectionProperties": "true", 
            "dbtable": "source.users_temp_1u3gbe", 
            "connectionName": "Redshift moodle sources connection"
            }, 
        transformation_ctx="tarjet_source_moodle_node1715523961675"
        )

job.commit()

# Comentarios

# Connections => mysql education,  Redshift moodle source connection

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED