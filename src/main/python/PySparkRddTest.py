from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .config("hive.metastore.uris", "thrift://10.106.4.19:9083") \
    .enableHiveSupport().getOrCreate()


df = spark.sql("""
SELECT ymd, chnl_id, clk_cnt
FROM ssa_brand.t_imp_clk_ssa
WHERE ymd >= '2021-05-01' and ymd <= '2021-05-31'
""")

rdd = df.rdd.map( lambda row : (row.ymd, ([row.chnl_id], [row.clk_cnt])))
reducedRdd = rdd.reduceByKey(lambda d1, d2 : d1 + d2)

def func(t):
    ymd = t[0]
    chnlList = t[1][0]
    clkCntList = t[1][1]
    listCnt = len(clkCntList)
    chnlCnt = len(set(chnlList))
    maxClkCnt = max(clkCntList)
    avgClkCnt = sum(clkCntList) / listCnt
    clkCntList.sort()
    medianClkCnt = clkCntList[listCnt/2]
    quaterClkCnt = clkCntList[listCnt/4]
    threeQuaterClkCnt = clkCntList[listCnt/4*3]
    return ymd + "," + str(chnlCnt) + "," + str(maxClkCnt) + "," + str(quaterClkCnt) + "," + str(medianClkCnt) + "," + str(threeQuaterClkCnt) + "," + str(avgClkCnt)

resultRdd = reducedRdd.map(lambda t: func(t))

resultRdd.toDF().write.mode("overwrite").text("/user/irteam/ikjuson/spark_perf/python_rdd")