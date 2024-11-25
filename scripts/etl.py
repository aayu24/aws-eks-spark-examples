from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split,col,to_timestamp,to_date,date_format,udf
from pyspark.sql.types import StringType
from spark_utils import get_spark_session
from LoadProperties import LoadProperties
from log_parser_utils import parse_log_line, extract_device
import sys

properties = LoadProperties()

def extract(spark: SparkSession, input_file: str):
    return spark.read.text(f"{input_file}")

def transform(logs_df: DataFrame):

    print("Showing a sample log statement")
    logs_df.show(1,truncate=False,vertical=True)

    print("Parsing logs")
    logs_parsed = logs_df.rdd.map(lambda row: parse_log_line(row.value)).filter(lambda x: x is not None).cache()

    logs_parsed_df = logs_parsed.toDF()

    print("Count after dropping duplicates")
    logs_parsed_df = logs_parsed_df.dropDuplicates()
    print(f"Num Records: {logs_parsed_df.count()}")

    print("Showing a parsed log")
    logs_parsed_df.show(1,truncate=False,vertical=True)

    print("Splitting request into method, endpoint, protocol")
    logs_parsed_df = logs_parsed_df.withColumn("method", split(col('request'), ' ').getItem(0))\
                                .withColumn("endpoint",split(col("request"), ' ').getItem(1))\
                                .withColumn("protocol", split(col("request"), ' ').getItem(2))

    print("Post splitting")
    logs_parsed_df.show(1,truncate=False,vertical=True)

    print("Formatting the datetime column as timestamp")
    timestamp_format = "dd/MMM/yyyy:HH:mm:ss Z"
    logs_parsed_df = logs_parsed_df.withColumn("timestamp", to_timestamp(col("datetime"), timestamp_format))

    logs_parsed_df.show(1,truncate=False,vertical=True)

    print("Extract request date for partitioning")
    logs_parsed_df = logs_parsed_df.withColumn("date",date_format(to_date(col("timestamp")),"yyyyMMdd"))
    logs_parsed_df.show(1,truncate=False,vertical=True)


    print("Extract device type from user agent")
    # Register a udf
    extract_device_udf = udf(extract_device, StringType())
    spark.udf.register("extract_device_udf",extract_device_udf)
    logs_parsed_df = logs_parsed_df.withColumn("device",extract_device_udf(col("useragent")))
    logs_parsed_df.cache()
    logs_parsed_df.show(1,truncate=False,vertical=True)

    return logs_parsed_df

def create_table(spark: SparkSession, db_name="demo", table_name="logs"):
    # Write To Apache Iceberg Table named logs
    query = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        date STRING,
        timestamp TIMESTAMP,
        datetime STRING,
        ip STRING,
        referrer STRING,
        request STRING,
        method STRING,
        endpoint STRING,
        protocol STRING,
        size STRING,
        status STRING,
        useragent STRING,
        device STRING
    ) USING ICEBERG 
    PARTITIONED BY (date)
    """
    print(query)
    spark.sql(query)

def load(spark: SparkSession, logs_parsed_df: DataFrame, db_name="demo", table_name="logs"):
    # Create Table First
    create_table(spark,db_name,table_name)
    # Write To Table
    logs_parsed_df.write.coalesce(3).format("iceberg").mode("overwrite").saveAsTable(f"{db_name}.{table_name}")

def execute(spark: SparkSession, input_file: str, db_name: str, table_name: str):
    load(spark,transform(extract(spark,input_file)),db_name,table_name)

if __name__ == "__main__":
    mode = None
    file = None
    if len(sys.argv) >= 3:
        mode=sys.argv[1]
        file=sys.argv[2]

    mode = properties.getMode() if mode is None else mode
    file = properties.getInputPath() if file is None else file

    print(f"Mode: {mode}, File: {file}")

    spark = get_spark_session(mode)
    execute(spark,file,properties.getCatalogName(),properties.getTableName())
    spark.stop()


