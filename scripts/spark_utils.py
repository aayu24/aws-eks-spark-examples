from pyspark import SparkConf
from pyspark.sql import SparkSession
from LoadProperties import LoadProperties
import os

properties = LoadProperties()

warehouse_path = properties.getWarehousePath()
iceberg_spark_jar  = properties.getIcebergSparkJar()
catalog_name = properties.getCatalogName()
catalog_type = properties.getCatalogType() # "hive"

def get_spark_session(mode="local"):
    
    if mode=="local":
        # Setup iceberg config
        conf = SparkConf().setAppName("Test") \
            .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .set('spark.jars.packages', iceberg_spark_jar) \
            .set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
            .set(f"spark.sql.catalog.{catalog_name}.type", catalog_type) \
            .set("spark.sql.warehouse.dir", warehouse_path) # To avoid creation of empty spark-warehouse folder
            # .set("spark.sql.defaultCatalog", catalog_name)
            # .set("hive.metastore.schema.verification", "false") \
            # .set("hive.metastore.schema.verification.record.version", "false")

        return SparkSession\
                .builder\
                .config(conf=conf)\
                .getOrCreate()
    
    elif mode=="sparksubmit":
        # All conf args would be passed from spark-args.conf file
        conf = SparkConf()\
            .set("spark.hadoop.fs.s3a.access.key",os.environ["AWS_ACCESS_KEY"])\
            .set("spark.hadoop.fs.s3a.secret.key",os.environ["AWS_SECRET_KEY"])

        spark = SparkSession\
                .builder\
                .config(conf=conf)\
                .getOrCreate()
        
        return spark