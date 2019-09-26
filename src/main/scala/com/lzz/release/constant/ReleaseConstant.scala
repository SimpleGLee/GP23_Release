package com.lzz.release.constant

import org.apache.spark.storage.StorageLevel

/**
  * Write By SimpleLee 
  * On 2019-九月-星期三
  * 10-41
  */
object ReleaseConstant {
  // TODO:partition-
  // TODO:内存+磁盘，优先内存
  val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  //分区
  val DEF_PARTITION = "bdp_day"

  val DEF_SOURCE_PARTITION = 4

  //维度列
  val COL_RELEASE_SESSION_STATUS:String = "release_status"

  //ODS================================================
  val ODS_RELEASE_SESSION:String = "ods_release.ods_01_release_session"

  //DW=================================================
  val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"
}
