package scalautils

import domain.{CountRecord, SessionRecord}
import org.apache.spark.sql.Row

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by sky on 2017/3/17.
  */
object Transformer extends Serializable{
    def rowsToSessionRecord(kv : (String, Iterable[Row])) : (String, SessionRecord) = {
        val record = new SessionRecord()
        val list = kv._2.toList

        // set sessionID|date|userID|cityID|userName|sex|cityName|name|age|professional
        if (list.nonEmpty){
            record.setSessionID(list(0).getString(2))
            record.setDate(list(0).getLong(1))
            record.setUserID(list(0).getLong(0))
            record.setCityID(list(0).getLong(12))
            record.setUserName(list(0).getString(13))
            record.setSex(list(0).getString(18))
            record.setCityName(list(0).getString(17))
            record.setName(list(0).getString(14))
            record.setAge(list(0).getInt(15))
            record.setProfessional(list(0).getString(16))
        }

        // set pageRecord|timestamps|searchWord|clickRecord|orderRecord|payRecord
        var index = 0
        val words = new ArrayBuffer[String]
        val click = new ArrayBuffer[SessionRecord.Product]
        val order = new ArrayBuffer[SessionRecord.Product]
        val pay = new ArrayBuffer[SessionRecord.Product]
        record.setPageRecord(new Array[Long](list.length))
        record.setTimestamps(new Array[Long](list.length))

        for (elem <- list){
            record.getPageRecord()(index) = elem.getLong(3)
            record.getTimestamps()(index) = elem.getLong(4)

            val word = elem.getString(5)
            if (word != "null") {
                words += word
            }
            if (!elem.isNullAt(6)){
                click += new SessionRecord.Product(elem.getLong(6), elem.getLong(7))
            }
            if (!elem.isNullAt(8)) {
                order += new SessionRecord.Product(elem.getLong(8), elem.getLong(9))
            }
            if (!elem.isNullAt(10)) {
                pay += new SessionRecord.Product(elem.getLong(10), elem.getLong(11))
            }
            index += 1
        }
        record.setSearchWord(words.toArray)
        record.setClickRecord(click.toArray)
        record.setOrderRecord(order.toArray)
        record.setPayRecord(pay.toArray)

        (kv._1, record)
    }

    def sessionRecordToCateRec(kv : (String, SessionRecord)) : Iterable[(Long, CountRecord)] = {
        var countMap = new HashMap[Long, CountRecord]
        def count(order : Int, product : SessionRecord.Product) : Unit = {
            if (!countMap.contains(product.category)){
                countMap += ((product.category, new CountRecord(0, 0, 0, product.category)))
            }
            countMap(product.category).addTime(order)
        }

        // count times
        kv._2.getClickRecord.foreach(count(1, _))
        kv._2.getOrderRecord.foreach(count(2, _))
        kv._2.getPayRecord.foreach(count(3, _))

        countMap
    }

    def sessionRowsToRecord(iterable: Iterable[Row]) : SessionRecord = {
        val record = new SessionRecord()
        val list = iterable.toList

        // set sessionID|date|userID|cityID
        if (list.nonEmpty){
            record.setSessionID(list(0).getString(2))
            record.setDate(list(0).getLong(0))
            record.setUserID(list(0).getLong(1))
            record.setCityID(list(0).getLong(12))
        }

        // set pageRecord|timestamps|searchWord|clickRecord|orderRecord|payRecord
        var index = 0
        val words = new ArrayBuffer[String]
        val click = new ArrayBuffer[SessionRecord.Product]
        val order = new ArrayBuffer[SessionRecord.Product]
        val pay = new ArrayBuffer[SessionRecord.Product]
        record.setPageRecord(new Array[Long](list.length))
        record.setTimestamps(new Array[Long](list.length))

        for (elem <- list){
            record.getPageRecord()(index) = elem.getLong(3)
            record.getTimestamps()(index) = elem.getLong(4)

            val word = elem.getString(5)
            if (word != "null") {
                words += word
            }
            if (!elem.isNullAt(6)){
                click += new SessionRecord.Product(elem.getLong(6), elem.getLong(7))
            }
            if (!elem.isNullAt(8)) {
                order += new SessionRecord.Product(elem.getLong(8), elem.getLong(9))
            }
            if (!elem.isNullAt(10)) {
                pay += new SessionRecord.Product(elem.getLong(10), elem.getLong(11))
            }
            index += 1
        }
        record.setSearchWord(words.toArray)
        record.setClickRecord(click.toArray)
        record.setOrderRecord(order.toArray)
        record.setPayRecord(pay.toArray)

        record
    }

    def addUserInfoToRecord(data : (SessionRecord, Row)) : SessionRecord = {
        // set userName|sex|cityName|name|age|professional
        data._1.setUserName(data._2.getString(1))
        data._1.setName(data._2.getString(2))
        data._1.setAge(data._2.getInt(3))
        data._1.setProfessional(data._2.getString(4))
        data._1.setCityName(data._2.getString(5))
        data._1.setSex(data._2.getString(6))

        data._1
    }
}
