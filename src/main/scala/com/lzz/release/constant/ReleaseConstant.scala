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

  //维度列
  val COL_RELEASE_SESSION_STATUS:String = "release_status"

}
