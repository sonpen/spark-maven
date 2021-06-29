package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]) {
    val conf = new SparkConf()
    conf.set("hive.metastore.uris", "thrift://10.106.4.19:9083")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.udf.register("UDF_TEST", udf_test(_: String) )

    val df = spark.sql(
      s"""
        |SELECT UDF_TEST(ymd), COUNT(DISTINCT chnl_id) AS chnl_cnt
        |       , MAX(clk_cnt) AS max_clk_cnt
        |       , PERCENTILE(clk_cnt, 0.75) AS 3_4_clk_cnt
        |       , PERCENTILE(clk_cnt, 0.5) AS median_clk_cnt
        |       , PERCENTILE(clk_cnt, 0.25) AS 1_4_clk_cnt
        |        , AVG(clk_cnt) AS avg_clk_cnt
        |FROM ssa_brand.t_imp_clk_ssa
        |WHERE ymd >= '2021-05-01' and ymd <= '2021-05-30'
        |GROUP BY UDF_TEST(ymd)
        |ORDER BY UDF_TEST(ymd)
        |""".stripMargin)

    df.write.mode("overwrite").format("csv").save("/user/irteam/ikjuson/spark_perf/scala")

  }

  def udf_test(str: String): String = {
    str.replace('-', '.')
  }
}
