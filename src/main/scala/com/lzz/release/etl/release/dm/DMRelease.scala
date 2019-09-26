//package com.lzz.release.etl.release.dm
//
//import com.lzz.release.etl.release.dw.DWReleaseColumnsHelper
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * Write By SimpleLee
//  * On 2019-九月-星期四
//  * 10-28
//  * 投放目标客户数据集市
//  */
//
//class DMRelease{
//
//}
//object DMRelease {
//  // TODO:日志
//
//  /**
//    * 统计目标客户集市
//    * @param spark
//    * @param appName
//    * @param bdp_day
//    */
//  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String)={
//    // TODO:导入内置函数和隐式转换
//
//
//    // TODO:缓存级别
//
//
//    // TODO:获取日志数据
//    val customerColumns: ArrayBuffer[String] = DWReleaseColumnsHelper.selectDWReleaseCustomerColumns()
//    // TODO:获取当天数据
//    col(s"${}")===lit(bdp_day)
//
//
//
//
//
//
//
//
//  }
//}
