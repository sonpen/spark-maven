from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def udf_test(ymd):
    return ymd.replace('-','.')

spark = SparkSession.builder \
    .config("hive.metastore.uris", "thrift://10.106.4.19:9083") \
    .enableHiveSupport().getOrCreate()

spark.udf.register("UDF_TEST", udf_test, StringType())

df = spark.sql("""
SELECT ymd AS ymd, COUNT(DISTINCT chnl_id) AS chnl_cnt
, MAX(clk_cnt) AS max_clk_cnt
, PERCENTILE(clk_cnt, 0.75) AS 3_4_clk_cnt
, PERCENTILE(clk_cnt, 0.5) AS median_clk_cnt
, PERCENTILE(clk_cnt, 0.25) AS 1_4_clk_cnt
, AVG(clk_cnt) AS avg_clk_cnt
, COUNT(UDF_TEST(ymd)) AS row_cnt
FROM ssa_brand.t_imp_clk_ssa
WHERE ymd >= '2021-05-01' and ymd <= '2021-05-31'
GROUP BY ymd
ORDER BY ymd""")

df.write.mode("overwrite").format("csv").save("/user/irteam/ikjuson/spark_perf/python")