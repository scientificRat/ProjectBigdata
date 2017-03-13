package scalautils

/**
  * Created by sky on 2017/3/11.
  */

import java.text.SimpleDateFormat
import java.util.Date

import conf.ConfigurationManager
import constants.Constants
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

object SparkUtils {

    /**
      * 加载本地测试数据到注册表
      *
      * @param sc
      * @param sqlContext
      */
    def loadLocalTestDataToTmpTable(sc: SparkContext, sqlContext: SQLContext): Unit = {

        //读入用户session日志数据并注册为临时表
        //首先指定临时表的Schema

        val sessionSchema = StructType(
            List(
                StructField("date", LongType, nullable = true), //0
                StructField("user_id", LongType, nullable = true), //1
                StructField("session_id", StringType, nullable = true), //2
                StructField("page_id", LongType, nullable = true), //3
                StructField("action_time", LongType, nullable = true), //4
                StructField("search_keyword", StringType, nullable = true), //5
                StructField("click_category_id", LongType, nullable = true), //6
                StructField("click_product_id", LongType, nullable = true), //7
                StructField("order_category_ids", StringType, nullable = true), //8
                StructField("order_product_ids", LongType, nullable = true), //9
                StructField("pay_category_ids", StringType, nullable = true), //10
                StructField("pay_product_ids", LongType, nullable = true), //11
                StructField("city_id", LongType, nullable = true) //12
            )
        )

        //从指定位置创建RDD
        val session_path = ConfigurationManager.getProperty(Constants.LOCAL_SESSION_DATA_PATH)
        val sessionRDD = sc.textFile(session_path).map(_.split(" "))
        //将RDD映射成rowRDD
        //fixme
        val sessionRowRDD = sessionRDD.map(s => Row(dateToLong(s(0)), strToLong(s(1)), s(2),
            strToLong(s(3)), timestampToLong(s(4) + " " + s(5)), s(6), strToLong(s(7)), strToLong(s(8)), s(9),
            strToLong(s(10)), s(11), strToLong(s(12)), strToLong(s(13))))
        //import sqlContext.implicits._

        //将schema信息应用到rowRDD上
        val sessionDataFrame = sqlContext.createDataFrame(sessionRowRDD, sessionSchema)

        //注册临时sessionAction表
        sessionDataFrame.registerTempTable(Constants.TABLE_USER_VISIT_ACTION)

        //定义用户数据schema
        val userSchema = StructType(
            List(
                StructField("user_id", LongType, nullable = true),
                StructField("username", StringType, nullable = true),
                StructField("name", StringType, nullable = true),
                StructField("age", IntegerType, nullable = true),
                StructField("professional", StringType, nullable = true),
                StructField("city", StringType, nullable = true),
                StructField("sex", StringType, nullable = true)
            )
        )
        //从指定位置创建RDD
        val user_path = ConfigurationManager.getProperty(Constants.LOCAL_USER_DATA_PATH)
        val userRDD = sc.textFile(user_path).map(_.split(" "))
        val userRowRdd = userRDD.map(u => Row(strToLong(u(0)), u(1), u(2), strToInt(u(3)), u(4), u(5), u(6)))
        val userDataFrame = sqlContext.createDataFrame(userRowRdd, userSchema)
        //注册用户信息临时表
        userDataFrame.registerTempTable(Constants.TABLE_USER_INFO)

        //定义商品数据schema
        val productSchema = StructType(
            List(
                StructField("product_id", LongType, nullable = true),
                StructField("product_title", StringType, nullable = true),
                StructField("extend_info", StringType, nullable = true)
            )
        )

        //从指定位置创建RDD
        val product_path = ConfigurationManager.getProperty(Constants.LOCAL_PRODUCT_DATA_PATH)
        val productRDD = sc.textFile(product_path).map(_.split(" "))
        val productRowRdd = userRDD.map(u => Row(strToLong(u(0)), u(1), u(2)))
        val productDataFrame = sqlContext.createDataFrame(productRowRdd, productSchema)
        //注册用户信息临时表
        productDataFrame.registerTempTable(Constants.TABLE_PRODUCT_INFO)

    }

    def timestampToLong(timeStr: String): Any = {
        if (timeStr == "null") null
        val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        df.parse(timeStr).getTime
    }

    def dateToLong(dateStr: String): Any = {
        if (dateStr == "null") null
        val df = new SimpleDateFormat("yyyy-MM-dd")
        df.parse(dateStr).getTime
    }

    def strToLong(str: String): Any = {
        if (str == "null") null else str.toLong
    }

    def strToInt(str: String): Any = {
        if (str == "null") null else str.toInt
    }

}