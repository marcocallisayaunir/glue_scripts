import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
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

# Script generated for node course_moodle
course_moodle_node1712090193041 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="moodle_education_moodle_campusvirtual_educacion_mdl_course", transformation_ctx="course_moodle_node1712090193041")

# Script generated for node courses_canvas
courses_canvas_node1712090146444 = glueContext.create_dynamic_frame.from_catalog(database="apify", table_name="micampus_micampus_public_course_dim", redshift_tmp_dir="s3://g-redshift-temp/temporal/", transformation_ctx="courses_canvas_node1712090146444")

# Script generated for node select_field_sisid_moodle
select_field_sisid_moodle_node1712090225939 = SelectFields.apply(frame=course_moodle_node1712090193041, paths=["idnumber"], transformation_ctx="select_field_sisid_moodle_node1712090225939")

# Script generated for node select_field_sisid_canvas
select_field_sisid_canvas_node1712090261973 = SelectFields.apply(frame=courses_canvas_node1712090146444, paths=["sis_source_id"], transformation_ctx="select_field_sisid_canvas_node1712090261973")

# Script generated for node filter_regex_sisid_moodle
filter_regex_sisid_moodle_node1712090278658 = Filter.apply(frame=select_field_sisid_moodle_node1712090225939, f=lambda row: (bool(re.match("^(\d+-)+\d+\|\d+\|\d+__\d+$", row["idnumber"]))), transformation_ctx="filter_regex_sisid_moodle_node1712090278658")

# Script generated for node filter_regex_sisid_canvas
filter_regex_sisid_canvas_node1712090286025 = Filter.apply(frame=select_field_sisid_canvas_node1712090261973, f=lambda row: (bool(re.match("^(\d+-)+\d+\|\d+\|\d+__\d+$", row["sis_source_id"]))), transformation_ctx="filter_regex_sisid_canvas_node1712090286025")

# Script generated for node query_selection_periods_moodle
SqlQuery2454 = '''
select
SUBSTRING_INDEX(idnumber, '|', 1) AS period,
SUBSTRING_INDEX(SUBSTRING_INDEX(idnumber, '|', 2), '|', -1) AS id_integration_subject
from moodlesource
'''
query_selection_periods_moodle_node1712090331937 = sparkSqlQuery(glueContext, query = SqlQuery2454, mapping = {"moodlesource":filter_regex_sisid_moodle_node1712090278658}, transformation_ctx = "query_selection_periods_moodle_node1712090331937")

# Script generated for node query_selection_periods_canvas
SqlQuery2452 = '''
select
SUBSTRING_INDEX(sis_source_id, '|', 1) AS period,
SUBSTRING_INDEX(SUBSTRING_INDEX(sis_source_id, '|', 2), '|', -1) AS id_integration_subject
from canvassource
'''
query_selection_periods_canvas_node1712090343843 = sparkSqlQuery(glueContext, query = SqlQuery2452, mapping = {"canvassource":filter_regex_sisid_canvas_node1712090286025}, transformation_ctx = "query_selection_periods_canvas_node1712090343843")

# Script generated for node query_split_periods_moodle
SqlQuery2457 = '''
SELECT 
    id_integration_subject,
    CASE WHEN size(split_period) >= 1 THEN split_period[0] ELSE NULL END AS period1,
    CASE WHEN size(split_period) >= 2 THEN split_period[1] ELSE NULL END AS period2,
    CASE WHEN size(split_period) >= 3 THEN split_period[2] ELSE NULL END AS period3,
    CASE WHEN size(split_period) >= 4 THEN split_period[3] ELSE NULL END AS period4,
    CASE WHEN size(split_period) >= 5 THEN split_period[4] ELSE NULL END AS period5,
    CASE WHEN size(split_period) >= 6 THEN split_period[5] ELSE NULL END AS period6,
    CASE WHEN size(split_period) >= 7 THEN split_period[6] ELSE NULL END AS period7,
    CASE WHEN size(split_period) >= 8 THEN split_period[7] ELSE NULL END AS period8,
    CASE WHEN size(split_period) >= 9 THEN split_period[8] ELSE NULL END AS period9,
    CASE WHEN size(split_period) >= 10 THEN split_period[9] ELSE NULL END AS period10
FROM (
    SELECT 
        id_integration_subject,
        period,
        SPLIT(period, '-') AS split_period
    FROM 
        enrollment_moodle
)
'''
query_split_periods_moodle_node1712090428373 = sparkSqlQuery(glueContext, query = SqlQuery2457, mapping = {"enrollment_moodle":query_selection_periods_moodle_node1712090331937}, transformation_ctx = "query_split_periods_moodle_node1712090428373")

# Script generated for node query_split_periods_canva
SqlQuery2453 = '''
SELECT 
    id_integration_subject,
    CASE WHEN size(split_period) >= 1 THEN split_period[0] ELSE NULL END AS period1,
    CASE WHEN size(split_period) >= 2 THEN split_period[1] ELSE NULL END AS period2,
    CASE WHEN size(split_period) >= 3 THEN split_period[2] ELSE NULL END AS period3,
    CASE WHEN size(split_period) >= 4 THEN split_period[3] ELSE NULL END AS period4,
    CASE WHEN size(split_period) >= 5 THEN split_period[4] ELSE NULL END AS period5,
    CASE WHEN size(split_period) >= 6 THEN split_period[5] ELSE NULL END AS period6,
    CASE WHEN size(split_period) >= 7 THEN split_period[6] ELSE NULL END AS period7,
    CASE WHEN size(split_period) >= 8 THEN split_period[7] ELSE NULL END AS period8,
    CASE WHEN size(split_period) >= 9 THEN split_period[8] ELSE NULL END AS period9,
    CASE WHEN size(split_period) >= 10 THEN split_period[9] ELSE NULL END AS period10
FROM (
    SELECT 
        id_integration_subject,
        period,
        SPLIT(period, '-') AS split_period
    FROM 
        enrollment_canvas
)
'''
query_split_periods_canva_node1712090450150 = sparkSqlQuery(glueContext, query = SqlQuery2453, mapping = {"enrollment_canvas":query_selection_periods_canvas_node1712090343843}, transformation_ctx = "query_split_periods_canva_node1712090450150")

# Script generated for node query_union_period_row_id_subject_canvas
SqlQuery2455 = '''
SELECT
    id_integration_subject,
    period1 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period1 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period2 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period2 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period3 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period3 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period4 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period4 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period5 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period5 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period6 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period6 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period7 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period7 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period8 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period8 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period9 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period9 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period10 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period10 IS NOT NULL;
'''
query_union_period_row_id_subject_canvas_node1712090569023 = sparkSqlQuery(glueContext, query = SqlQuery2455, mapping = {"myDataSource":query_split_periods_moodle_node1712090428373}, transformation_ctx = "query_union_period_row_id_subject_canvas_node1712090569023")

# Script generated for node query_union_period_row_id_subject_moodle
SqlQuery2456 = '''
SELECT
    id_integration_subject,
    period1 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period1 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period2 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period2 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period3 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period3 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period4 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period4 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period5 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period5 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period6 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period6 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period7 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period7 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period8 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period8 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period9 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period9 IS NOT NULL

UNION ALL

SELECT
    id_integration_subject,
    period10 AS id_integration_enrollment
FROM
    myDataSource
WHERE
    period10 IS NOT NULL;
'''
query_union_period_row_id_subject_moodle_node1712090596692 = sparkSqlQuery(glueContext, query = SqlQuery2456, mapping = {"myDataSource":query_split_periods_canva_node1712090450150}, transformation_ctx = "query_union_period_row_id_subject_moodle_node1712090596692")

# Script generated for node schema_matcher_for_moodle
schema_matcher_for_moodle_node1712091975613 = ApplyMapping.apply(frame=query_union_period_row_id_subject_canvas_node1712090569023, mappings=[("id_integration_subject", "string", "id_integration_subject", "string"), ("id_integration_enrollment", "string", "id_integration_enrollment", "string")], transformation_ctx="schema_matcher_for_moodle_node1712091975613")

# Script generated for node schema_matcher_for_canva
schema_matcher_for_canva_node1712092028329 = ApplyMapping.apply(frame=query_union_period_row_id_subject_moodle_node1712090596692, mappings=[("id_integration_subject", "string", "id_integration_subject", "string"), ("id_integration_enrollment", "string", "id_integration_enrollment", "string")], transformation_ctx="schema_matcher_for_canva_node1712092028329")

# Script generated for node union_canvas_moodle
union_canvas_moodle_node1712090633715 = sparkUnion(glueContext, unionType = "DISTINCT", mapping = {"source1": schema_matcher_for_moodle_node1712091975613, "source2": schema_matcher_for_canva_node1712092028329}, transformation_ctx = "union_canvas_moodle_node1712090633715")

# Script generated for node redshift_union_export_canvas_moodle
redshift_union_export_canvas_moodle_node1712090707620 = glueContext.write_dynamic_frame.from_options(frame=union_canvas_moodle_node1712090633715, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-760557048868-eu-west-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.enrollment_period", "connectionName": "Redshift Cluster Apify", "preactions": "DROP TABLE IF EXISTS public.enrollment_period; CREATE TABLE IF NOT EXISTS public.enrollment_period (id BIGINT PRIMARY KEY IDENTITY(1,1), id_integration_subject VARCHAR, id_integration_enrollment VARCHAR);"}, transformation_ctx="redshift_union_export_canvas_moodle_node1712090707620")

job.commit()


# Comentarios

# Connections => mysql Moodle test,  Redshift cluster apify, redshift micampues

# --conf => spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED
