package com.lzz.release.etl.release.dw

import com.lzz.release.constant.ReleaseConstant
import com.lzz.release.enums.ReleaseStatusEnum
import com.lzz.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * Write By SimpleLee 
  * On 2019-九月-星期三
  * 10-29
  * DW 投放目标用户主题
  */

class DWReleaseCustomer {

}

object DWReleaseCustomer {

  // TODO:日志处理
  private val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

  /**
    * 目标客户
    * 01
    */
  def handleReleaseJob(sparkSession:SparkSession,appName:String,bdp_day:String)={
    //获取当前时间
    val begin: Long = System.currentTimeMillis()
    try{
      // TODO:导入隐式转换
      import sparkSession.implicits._
      import org.apache.spark.sql.functions._

      // TODO:设置缓存级别--统计dm时使用缓存
      val storagelevel = ReleaseConstant.DEF_STORAGE_LEVEL
      //

      //获取日志字段数据
      val customerColumns: ArrayBuffer[String] = DWReleaseColumnsHelper.selectDWReleaseCustomerColumns()
      //设置条件 当天数据 获取目标客户：01 lit()-自变量-返回一个列   列与列之间使用===
      val customerReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
        and
        col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit(ReleaseStatusEnum.CUSTOMER.getCode)
        )

      //
      val customerReleaseDF: DataFrame = SparkHelper.readTableData(sparkSession,ReleaseConstant.ODS_RELEASE_SESSION,customerColumns)
      //填入条件
        .where(customerReleaseCondition)
      //重分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)

      println("DWReleaseDF*********************************************************")

      // TODO:打印查看结果
      customerReleaseDF.show(10,false)

      // 目标用户（存储）
      //SparkHelper.writeTableData(customerReleaseDF,ReleaseConstant.DW_RELEASE_CUSTOMER,saveMode)

    }catch{
      //错误信息处理
      case ex:Exception => {
        logger.error(ex.getMessage,ex)
      }
    }finally {
      //任务处理的时长
      s"任务处理时长：${appName},bdp_day=${bdp_day},${System.currentTimeMillis()-begin}"
    }

  }

  /**
    * 投放目标用户
    * @param appName
    * @param bdp_day_begin
    * @param bdp_day_end
    */
  def handleJobs(appName:String,bdp_day_begin:String,bdp_day_end:String)={
    var spark:SparkSession =null
    try{
      // 配置Spark参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName(appName)
        .setMaster("local[*]")
      // 创建上下文
      spark = SparkHelper.createSpark(conf)
      // 解析参数
      val timeRange = SparkHelper.rangeDates(bdp_day_begin,bdp_day_end)
      // 循环参数
      for(bdp_day <- timeRange){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark,appName,bdp_date)
      }
    }catch {
      case ex:Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val appName = "dw_release_job"
    val bdp_day_begin = "2019-09-24"
    val bdp_day_end = "2019-09-25"
    //执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }

}
