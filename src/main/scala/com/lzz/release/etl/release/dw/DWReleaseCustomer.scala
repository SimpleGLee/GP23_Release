package com.lzz.release.etl.release.dw

import com.lzz.release.constant.ReleaseConstant
import org.apache.spark.sql.SparkSession
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
        col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") === lit()
        )

      //


    }catch(){

    }



  }


}
