import sys
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.job import Job

arg_keys =  ['JOB_NAME', 'connection_name', 'bucket_root']
args = getResolvedOptions(sys.argv, arg_keys)

(job_name, connection_name, bucket_root) = [args[k] for k in arg_keys]

now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(job_name, args)

jdbc_conf = glueContext.extract_jdbc_conf(connection_name)

## transformするカラム一覧の抽出
transformed_columns_dyf = glueContext.create_dynamic_frame_from_options('mysql', connection_options={
        "url": "{0}/{1}".format(jdbc_conf['url'], 'information_schema'), "user": jdbc_conf['user'], "password": jdbc_conf['password'], "dbtable": "COLUMNS"
    }
)

## 除外スキーマを除いて、変換対象の型のカラムに絞り込む
transformed_columns = transformed_columns_dyf \
    .toDF() \
    .filter( \
        "(DATA_TYPE = 'datetime' \
            OR DATA_TYPE = 'decimal' \
            OR DATA_TYPE = 'date' \
        ) \
        AND TABLE_SCHEMA != 'mysql' \
        AND TABLE_SCHEMA != 'information_schema' \
        AND TABLE_SCHEMA != 'performance_schema' \
        AND TABLE_SCHEMA != 'sys'" \
    ).collect()

## transformリスト生成（スキーマ・テーブル・カラムの組み合わせ）
transformed_columns_list = {}

for row in transformed_columns:
    if row['TABLE_SCHEMA'] not in transformed_columns_list:
        transformed_columns_list[row['TABLE_SCHEMA']] = {}
    if row['TABLE_NAME'] not in transformed_columns_list[row['TABLE_SCHEMA']]:
        transformed_columns_list[row['TABLE_SCHEMA']][row['TABLE_NAME']] = []
        
    row_col_type = {}
    row_col_type['COLUMN_NAME'] = row['COLUMN_NAME']
    row_col_type['DATA_TYPE'] = row['DATA_TYPE']

    transformed_columns_list[row['TABLE_SCHEMA']][row['TABLE_NAME']].append(row_col_type)
    
    
## ETL対象のスキーマ・テーブル一覧を取得するDynamicFrame生成
target_tables_dyf = glueContext.create_dynamic_frame_from_options(
  'mysql', connection_options={
    "url": "{0}/{1}".format(jdbc_conf['url'], 'information_schema'), "user": jdbc_conf['user'], "password": jdbc_conf['password'], "dbtable": "TABLES"
  }
)

## いらないスキーマを弾いて取得
target_tables = target_tables_dyf.toDF().filter("TABLE_SCHEMA != 'mysql' AND TABLE_SCHEMA != 'information_schema' AND TABLE_SCHEMA != 'performance_schema' AND TABLE_SCHEMA != 'sys'").collect()

## DB-TableごとにS3書き込み
for row in target_tables:
    db_schema = row['TABLE_SCHEMA']
    table_name = row['TABLE_NAME']
    
    ## Tableのカラム一覧抽出
    mapping_type_dyf = glueContext.create_dynamic_frame_from_options(
      'mysql', connection_options={
        "url": "{0}/{1}".format(jdbc_conf['url'], 'information_schema'), "user": jdbc_conf['user'], "password": jdbc_conf['password'], "dbtable": "COLUMNS"
      }
    )
    
    mapping_type = mapping_type_dyf.toDF().filter("TABLE_SCHEMA = '" + db_schema + "' AND TABLE_NAME = '" + table_name + "'").collect()

    ## 型のマッピングルール定義
    mappings_list = []
    choice_list = []
    for map_type in mapping_type:
        if map_type['DATA_TYPE'] == "int":
            map_source_type = "int"
            map_target_type = "int"
        elif map_type['DATA_TYPE'] == "bigint":
            map_source_type = "bigint"
            map_target_type = "long"
        elif map_type['DATA_TYPE'] == "tinyint":
            map_source_type = "tinyint"
            map_target_type = "byte"
        elif map_type['DATA_TYPE'] == "varbinary":
            map_source_type = "binary"
            map_target_type = "binary"
        elif map_type['DATA_TYPE'] == "decimal":
            map_source_type = "decimal"
            map_target_type = "decimal"
        elif map_type['DATA_TYPE'] == "date":
            map_source_type = "date"
            map_target_type = "date"
        elif map_type['DATA_TYPE'] == "datetime":
            map_source_type = "timestamp"
            map_target_type = "timestamp"
        else:
            map_source_type = "string"
            map_target_type = "string"
            
        choice_tuple = (map_type['COLUMN_NAME'], "cast:" + map_source_type)
        choice_list.append(choice_tuple)
            
        mapping_tuple = (map_type['COLUMN_NAME'], map_source_type, map_type['COLUMN_NAME'], map_target_type)
        mappings_list.append(mapping_tuple)

    ## 実テーブルからデータ取得
    table_result_dyf = glueContext.create_dynamic_frame_from_options('mysql', connection_options={
            "url": "{0}/{1}".format(jdbc_conf['url'], db_schema), "user": jdbc_conf['user'], "password": jdbc_conf['password'], "dbtable": table_name
        }
    )
    
    ## 型変換
    resolvechoice = ResolveChoice.apply(frame = table_result_dyf, specs = choice_list, transformation_ctx = "resolvechoice")
    applymapping = ApplyMapping.apply(frame = resolvechoice, mappings = mappings_list, transformation_ctx = "applymapping")
    
    ## 実データのDataFrameに変換
    table_result = applymapping.toDF()

    ## 実データ変換
    if db_schema in transformed_columns_list and table_name in transformed_columns_list[db_schema]:
        for col_type in transformed_columns_list[row['TABLE_SCHEMA']][row['TABLE_NAME']]:
            if col_type['DATA_TYPE'] == "decimal":
                table_result = table_result.withColumn( \
                    col_type['COLUMN_NAME'], \
                    regexp_extract(
                        col_type['COLUMN_NAME'], \
                        '([0-9]+\.[0-9])0$', \
                        1 \
                    ) \
                )
            elif col_type['DATA_TYPE'] == "datetime":
                table_result = table_result.withColumn( \
                    col_type['COLUMN_NAME'], \
                    regexp_replace( \
                        col_type['COLUMN_NAME'], \
                        '\.0$', \
                        '' \
                    ) \
                )

            if col_type['DATA_TYPE'] == "datetime":
                from_data = '0000-00-00 00:00:00'
                to_data = None
            elif col_type['DATA_TYPE'] == "date":
                from_data = '0000-00-00'
                to_data = None
            else:
                continue

            table_result = table_result.withColumn( \
                col_type['COLUMN_NAME'], \
                when( \
                    col(col_type['COLUMN_NAME']) == from_data, \
                        to_data \
                    ).otherwise( \
                        col(col_type['COLUMN_NAME']) \
                ) 
            )
            
    transformed_data_dyf = applymapping.fromDF(table_result, glueContext, "transformed_data_dyf")
    
    glueContext.write_dynamic_frame.from_options(
        frame=transformed_data_dyf,
        connection_type="s3",
        connection_options={"path": bucket_root + "/" + now + "/" + db_schema + "/" + table_name},
        format="json"
    )
    
job.commit()