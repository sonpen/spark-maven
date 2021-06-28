from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("hive.metastore.uris", "thrift://10.106.4.19:9083") \
    .enableHiveSupport().getOrCreate()

df = spark.sql("""
select *
from ssa_brand.t_imp_clk_ssa
where ymd = '2021-06-12'
""").show()