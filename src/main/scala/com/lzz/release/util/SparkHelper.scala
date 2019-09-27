package com.lzz.release.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Write By SimpleLee 
  * On 2019-九月-星期三
  * 11-22
  * Spark工具
  */
object SparkHelper {

  private val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

  /**
    * 读取数据
    * @param sparkSession
    * @param tableName
    * @param colNames
    * @return DataFrame
    */
  def readTableData(sparkSession:SparkSession,tableName:String,colNames:mutable.Seq[String]):DataFrame={
    import sparkSession.implicits._
    //获取数据
    val tableDF: DataFrame = sparkSession.read.table(tableName)
      .selectExpr(colNames: _*)
    tableDF
  }

  /**
    * 写入数据
    * @param sourceDF
    * @param table
    * @param mode
    */
  def writeTableData(sourceDF:DataFrame,table:String,mode:SaveMode)={
    //写入表
    sourceDF.write.mode(mode).insertInto(table)
  }

  /**
    * 创建SparkSession
    * @param conf
    * @return
    */
  def createSpark(conf:SparkConf):SparkSession={
    val sparkSession: SparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    //调用自定义函数


    sparkSession
  }

  /**
    * 参数校验
    * @param begin
    * @param end
    * @return
    */
  def rangeDates(begin:String,end:String):Seq[String]={
    val bdp_days = new ArrayBuffer[String]()
    try{
      val bdp_date_begin: String = DateUtil.dateFormat2String(begin,"yyyy-MM-dd")
      val bdp_date_end: String = DateUtil.dateFormat2String(end,"yyyy-MM-dd")
      //如果两个时间相等，取其中的第一个开始时间
      //如果不相等，计算时间差
      if(begin.equals(end)){
        bdp_days.+=(bdp_date_begin)
      }else{
        var cday = bdp_date_begin
        while (cday<bdp_date_end){
          bdp_days.+=(cday)
          //让初始时间累加，以天为单位
          val pday: String = DateUtil.dateFormat2StringDiff(cday,1,"yyyy-MM-dd")
          cday = pday
        }
      }
    }catch{
      case ex:Exception => {
        println("参数不匹配")
        logger.error(ex.getMessage,ex)
      }
    }

    bdp_days

  }



}
