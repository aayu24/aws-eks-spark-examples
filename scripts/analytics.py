from pyspark.sql import SparkSession, DataFrame
from spark_utils import get_spark_session
from LoadProperties import LoadProperties
import sys

properties = LoadProperties()
db_name = properties.getCatalogName()
table_name = properties.getTableName()

def save_results(df: DataFrame, output_path: str):
    df.coalesce(1)\
        .write\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .csv(output_path)
    
def fetch_results(spark: SparkSession, query: str):
    df = spark.sql(query)
    df.show()
    return df

def execute_query(spark: SparkSession, query: str, output_path: str):
    save_results(fetch_results(spark,query),output_path)

# Top 5 ip addresses by request count
top5_ips_query = f"""with day_ip_cnt as (
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
order by date desc,hits desc"""
          
top5_devices_query = f"""with day_device_cnt as (
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
order by date desc,hits desc"""

weekly_top5_ips_query = f"""with week_ip_cnt as (
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
order by week_date desc,hits desc"""

weekly_top5_devices_query = f"""with week_device_cnt as (
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
order by week_date desc,hits desc"""

if __name__ == "__main__":
    if len(sys.argv)>= 1:
        mode = sys.argv[1]
    mode = "local" if mode is None else mode
    spark = get_spark_session(mode)
    queries = [{"query": top5_ips_query, "path": "output/top5_ips_daily"}, 
               {"query" : top5_devices_query, "path": "output/top5_devices_daily"}, 
               {"query": weekly_top5_ips_query, "path": "output/top5_ips_weekly"}, 
               {"query": weekly_top5_devices_query, "path": "output/top5_devices_weekly"}]
    for query in queries:
        execute_query(spark, query["query"],query["path"])
    