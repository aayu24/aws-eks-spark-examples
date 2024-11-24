from pyspark import SparkConf
from pyspark.sql import SparkSession

warehouse_path = "warehouse"
iceberg_spark_jar  = 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0'
catalog_name = "demo"
catalog_type = "hadoop" # "hive"
db_name = "demo"
table_name = "logs"

# Setup iceberg config
conf = SparkConf().setAppName("Test") \
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .set('spark.jars.packages', iceberg_spark_jar) \
    .set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
    .set(f"spark.sql.catalog.{catalog_name}.type", catalog_type)\
    .set("spark.sql.defaultCatalog", catalog_name)

spark = SparkSession\
    .builder\
    .config(conf=conf)\
    .getOrCreate()


# Top 5 ip addresses by request count
top5_ips_df = spark.sql(f"""with day_ip_cnt as (
    select date,ip,count(1) as hits
    from {db_name}.{table_name}
    group by date,ip
),
ranked_day_ip_cnt as (
    select date,ip,hits,
    row_number() over (partition by date order by hits desc) as rn
    from day_ip_cnt
)
select date,ip,hits from ranked_day_ip_cnt
where rn<=5
order by date desc,hits desc""")

top5_ips_df.show()

# Write DataFrame to CSV File
output_path = "output/top5_ips_daily"  # Specify your output path

top5_ips_df.coalesce(1)\
    .write\
    .option("header", True)\
    .option("delimiter", ";")\
    .mode("overwrite")\
    .csv(output_path)               


top5_devices_df = spark.sql(f"""with day_device_cnt as (
    select date,device,count(1) as hits 
    from {db_name}.{table_name}
    group by date,device
),
ranked_day_device_cnt as (
    select date,device,hits,
    row_number() over (partition by date order by hits desc) as rn
    from day_device_cnt
)
select date,device,hits from ranked_day_device_cnt
where rn<=5
order by date desc,hits desc""")

top5_devices_df.show()

# Write DataFrame to CSV File
output_path = "output/top5_devices_daily"  # Specify your output path

top5_devices_df.coalesce(1)\
    .write\
    .option("header", True)\
    .option("delimiter", ";")\
    .mode("overwrite")\
    .csv(output_path)

print("Showing the Weekly Analytical Queries")

weekly_top5_ips_df = spark.sql(f"""with week_ip_cnt as (
    select DATE_SUB(DATE(timestamp), (DAYOFWEEK(timestamp) - 1)) as week_date,ip,count(1) as hits
    from {db_name}.{table_name}
    group by DATE_SUB(DATE(timestamp), (DAYOFWEEK(timestamp) - 1)),ip
),
ranked_week_ip_cnt as (
    select week_date,ip,hits,
    row_number() over (partition by week_date order by hits desc) as rn
    from week_ip_cnt
)
select week_date,ip,hits from ranked_week_ip_cnt
where rn<=5
order by week_date desc,hits desc""")

weekly_top5_ips_df.show()

# Write DataFrame to CSV File
output_path = "output/top5_ips_weekly"  # Specify your output path

weekly_top5_ips_df.coalesce(1)\
    .write\
    .option("header", True)\
    .option("delimiter", ";")\
    .mode("overwrite")\
    .csv(output_path)

weekly_top5_devices_df = spark.sql(f"""with week_device_cnt as (
    select DATE_SUB(DATE(timestamp), (DAYOFWEEK(timestamp) - 1)) as week_date,device,count(1) as hits
    from {db_name}.{table_name}
    group by DATE_SUB(DATE(timestamp), (DAYOFWEEK(timestamp) - 1)),device
),
ranked_week_device_cnt as (
    select *,
    row_number() over (partition by week_date order by hits desc) as rn
    from week_device_cnt
)
select week_date,device,hits from ranked_week_device_cnt
where rn<=5
order by week_date desc,hits desc""")

weekly_top5_devices_df.show()

# Write DataFrame to CSV File
output_path = "output/top5_devices_weekly"  # Specify your output path

weekly_top5_devices_df.coalesce(1)\
    .write\
    .option("header", True)\
    .option("delimiter", ";")\
    .mode("overwrite")\
    .csv(output_path)