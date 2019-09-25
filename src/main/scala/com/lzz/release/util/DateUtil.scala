package com.lzz.release.util

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
  * Write By SimpleLee 
  * On 2019-九月-星期三
  * 14-10
  * 数据处理工具类
  */
object DateUtil {

  def dateFormat2String(date:String,formater:String="yyyyMMdd"):String={

    if(null == date){
      return null
    }

    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(formater)

    val localDate: LocalDate = LocalDate.parse(date,formatter)

    //校验时间参数
    localDate.format(DateTimeFormatter.ofPattern(formater))

  }

  def dateFormat2StringDiff(date:String,diff:Long,fromater:String="yyyyMMdd"):String={
    if(null == date){
      return null
    }
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(fromater)

    val localDate: LocalDate = LocalDate.parse(date,formatter)


    //处理天的累加
    val resultDateTime: LocalDate = localDate.plusDays(diff)

    resultDateTime.format(DateTimeFormatter.ofPattern(fromater))

  }

}
