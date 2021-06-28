package org.example

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

case class AggrData(chnlList: ArrayBuffer[String], clkCntList: ArrayBuffer[Long])
object RddApp {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder().getOrCreate()

    val df = spark.read.orc("hdfs://matching/user/hive/warehouse/ssa_brand.db/t_imp_clk_ssa/ymd=2021-05-*")
    val rdd = df.rdd.map{ row =>
      val chnlId = row.getAs[String]("chnl_id")
      val clkCnt = row.getAs[Long]("clk_cnt")
      val ymd = row.getAs[String]("ymd")

      (ymd, AggrData(ArrayBuffer(chnlId), ArrayBuffer(clkCnt)))
    }

    val reducedRdd = rdd.reduceByKey((d1, d2) => AggrData(d1.chnlList ++= d2.chnlList, d1.clkCntList ++= d2.clkCntList))
    val resultRdd = reducedRdd.map{ t =>
      val ymd = t._1
      val aggrData = t._2

      val listCnt = aggrData.clkCntList.length

      val chnlCnt = aggrData.chnlList.distinct.length
      val maxClkCnt = aggrData.clkCntList.reduce((a,b) => if( a > b) a else b)
      val avgClkCnt = aggrData.clkCntList.reduce((a, b) => a+b) / listCnt
      val sortedList = aggrData.clkCntList.sorted

      val medianClkCnt = sortedList(listCnt/2)
      val quaterClkCnt = sortedList(listCnt/4)
      val threeQuaterClkCnt = sortedList(listCnt/4*3)

      ymd + "," + chnlCnt + "," + maxClkCnt + "," + quaterClkCnt + "," + medianClkCnt + "," + threeQuaterClkCnt + "," + avgClkCnt)
    }

    import spark.implicits._
    resultRdd.toDF().as[String].write.mode("overwrite").text("/user/irteam/ikjuson/spark_perf/scala_rdd")
  }

}
