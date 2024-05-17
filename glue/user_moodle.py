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

## Variables ##

moodle_instances = [
    {"nombre": "moodle_education_moodle_campusvirtual_educacion", "fuente": "1"},
    {"nombre": "moodletest_lms_moodle_des", "fuente": "2"}
    ]
for instance in moodle_instances:
        

    source_user_node1714502166743 =  glueContext.create_dynamic_frame.from_catalog(database="apify", table_name=instance['nombre'] + "_mdl_user", transformation_ctx="source_user_node1714502166743")

        
        # Segmento para agregar campo adicional con valor por defecto
    spark_df2 = source_user_node1714502166743.toDF();
    dfNewColumn2 = spark_df2.withColumn('id_data_origin', lit(instance['fuente']));
    source_user_node1714502166743 = DynamicFrame.fromDF(dfNewColumn2, glueContext, "convert");
        # Segmento para agregar campo adicional con valor por defecto
    
        
        # Script generated for node select_fields
    select_fields_node1714502290041 = SelectFields.apply(frame=source_user_node1714502166743, paths=["id", "idnumber", "username", "deleted", "timecreated", "id_data_origin"], transformation_ctx="select_fields_node1714502290041")
        
        # Script generated for node query_select_filter
    SqlQuery2616 = '''
    select
        
    idnumber AS id_integration_proeduca_user,
    username AS user_login,
    id AS id_internal_user,
    id_data_origin,
    TO_TIMESTAMP(timecreated) AS created_at,
    CURRENT_TIMESTAMP AS updated_at
        
    from myDataSource where deleted = 0
    '''
        
        
    query_select_filter_node1714502576513 = sparkSqlQuery(glueContext, query = SqlQuery2616, mapping = {"myDataSource":select_fields_node1714502290041}, transformation_ctx = "query_select_filter_node1714502576513")
        
             # Script generated for node Change Schema
    ChangeSchema_node1710952459869 = ApplyMapping.apply(frame=query_select_filter_node1714502576513, mappings=[("id_integration_proeduca_user", "string", "id_integration_proeduca_user", "string"), ("user_login", "string", "user_login", "string"),  ("id_internal_user", "long", "id_internal_user", "long"), ("id_data_origin", "string", "id_data_origin", "long"), ("created_at", "TIMESTAMP", "created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP", "updated_at", "TIMESTAMP") ], transformation_ctx="ChangeSchema_node1710952459869")
    
        
        # Script generated for node Amazon Redshift
    AmazonRedshift_node1714504196170 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1710952459869, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.users", "connectionName": "Redshift moodle sources connection"}, transformation_ctx="AmazonRedshift_node1714504196170")
    
job.commit()

# Comentarios

# Connections => mysql education,  Redshift moodle source connection

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED