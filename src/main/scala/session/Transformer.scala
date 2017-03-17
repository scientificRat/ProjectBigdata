package session

import domain.SessionRecord
import org.apache.spark.sql.Row

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
}
