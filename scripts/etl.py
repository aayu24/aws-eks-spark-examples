import re
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,to_timestamp,to_date,date_format,udf
from pyspark.sql.types import StringType
from user_agents import parse


# LOG_FORMAT = r'^(?P<client_ip>\S+) - - \[(?P<datetime>[^\]]+)\] "(?P<method>[A-Z]+) (?P<request>[^ ]+) HTTP/[0-9.]+" (?P<status_code>\d{3}) (?P<size>\d+) "-" "(?P<user_agent>[^"]*)"'
LOG_FORMAT = re.compile(r'^(?P<ip>\S+) \S+ \S+ \[(?P<datetime>.*?)\] "(?P<request>.*?)" (?P<status>\d{3}) (?P<size>\S+) "(?P<referrer>[^"]*)" "(?P<useragent>[^"]*)"')

def parse_log_line(logline):
    match = LOG_FORMAT.match(logline)
    if match is None:
        print(f"Given {logline} logline could not be parsed")
        return None
    return match.groupdict()

@udf(StringType())
def extract_device(ua_string):
    user_agent = parse(ua_string)
    if user_agent.is_mobile:
        return "Mobile"
    elif user_agent.is_tablet:
        return "Tablet"
    elif user_agent.is_pc:
        return "PC"
    elif user_agent.is_bot:
        return "Bot"
    elif user_agent.is_email_client:
        return "EmailClient"
    else:
        return "NA"
    


warehouse_path = "warehouse"
iceberg_spark_jar  = 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0'
catalog_name = "demo"
catalog_type = "hadoop" # "hive"

# Setup iceberg config
conf = SparkConf().setAppName("Test") \
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .set('spark.jars.packages', iceberg_spark_jar) \
    .set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
    .set(f"spark.sql.catalog.{catalog_name}.type", catalog_type)
    # .set("spark.sql.warehouse.dir", warehouse_path)\
    # .set("spark.sql.defaultCatalog", catalog_name)
    # .set("hive.metastore.schema.verification", "false") \
    # .set("hive.metastore.schema.verification.record.version", "false")

spark = SparkSession\
    .builder\
    .config(conf=conf)\
    .getOrCreate()

logs_df = spark.read.text("really_large_access.log")

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
logs_parsed_df = logs_parsed_df.withColumn("device",extract_device(col("useragent")))
logs_parsed_df.cache()
logs_parsed_df.show(1,truncate=False,vertical=True)

# Write To Apache Iceberg Table named logs
db_name = "demo"
table_name = "logs"

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


# Create Table First
logs_parsed_df.write.format("iceberg").mode("overwrite").saveAsTable(f"{db_name}.{table_name}")
# logs_parsed_df.write.format("iceberg").mode("overwrite").save("warehouse/logs_vanilla")
spark.stop()