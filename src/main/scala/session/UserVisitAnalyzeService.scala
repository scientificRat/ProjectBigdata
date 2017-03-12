package session

import java.sql.Connection

import com.google.gson.Gson
import constants.Constants
import dao.DBHelper
import domain.{ProductStat, UserInput}
import exception.UserInputException
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scalautils.SparkUtils

/**
  * Created by sky on 2017/3/11.
  */
object UserVisitAnalyzeService {
  def main(args: Array[String]): Unit = {
    // 配置Spark
    val sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster("local[4]")
    // 负责和集群通信
    val sparkContext = new SparkContext(sparkConf)
    // spark sql是建立在sparkCores上面的，那么自然而然需要使用到sparkContext进行通信
    val sqlContext = new SQLContext(sparkContext)

    // 获取输入
    val dbConnection = DBHelper.getConnection
    val userInput = getUserInput(dbConnection)
    // 读取数据
    SparkUtils.loadLocalTestDataToTmpTable(sc = sparkContext, sqlContext = sqlContext)

    if (userInput.getTaskID == "1") {
      // 在指定日期范围内，按照session粒度进行数据聚合
      // 检查输入合法性
      if (userInput.getStartDate == null) {
        println("error,parameter startDate is required")
        return
      }
      if (userInput.getEndDate == null) {
        println("error,parameter endDate is required")
        return
      }

      val rdd = sqlContext
        .sql(s"select * from ${Constants.TABLE_USER_VISIT_ACTION} natural join ${Constants.TABLE_USER_INFO} WHERE date >= '${userInput.getStartDate.getTime}' AND date<= '${userInput.getEndDate.getTime}'")
        .rdd.map(row => {
        (row.get(2), formatToValue(row))
      })
      rdd.foreach(println)

    } else if (userInput.getTaskID == "2") {
      // 根据用户的查询条件 返回的结果RDD, 一个或者多个：年龄范围，职业（多选），城市（多选），搜索词（多选），点击品类（多选）进行数据过滤,session时间范围是必选的

      val limit = try {
        constructSqlLimitHelp(userInput)
      } catch {
        case e: Exception => println(e.getMessage)
          return
      }

      val sql = s"select * from ${Constants.TABLE_USER_VISIT_ACTION} natural join ${Constants.TABLE_USER_INFO} WHERE date >= '${userInput.getStartDate.getTime}' AND date<= '${userInput.getEndDate.getTime}' " + limit
      val rdd = sqlContext.sql(sql).rdd.map(row => {
        (row.get(2), formatToValue(row))
      })
      rdd.foreach(println)

    } else if (userInput.getTaskID == "3") {
      // 实现自定义累加器完成多个聚合统计业务的计算,统计业务包括访问时长：1~3秒，4~6秒，7~9秒，10~30秒，30~60秒的session访问量统计，访问步长：1~3个页面，4~6个页面等步长的访问统计
      // -----------------------------------

      var sql_timebase_stat = s"select count(*) as count from ${Constants.TABLE_USER_VISIT_ACTION} group by session_id having (max(action_time)-min(action_time)) "
      var sql_pagebase_stat = s"select session_id from ${Constants.TABLE_USER_VISIT_ACTION} group by session_id having count(*) "

      def generateSQLRangeStr(low: Int, high: Int): String = {
        s"between $low and $high"
      }

      def doTimeBasedStatInRange(low: Int, high: Int): Unit = {
        // 1000表示从 ms --> s
        val tmp = sqlContext.sql(sql_timebase_stat + generateSQLRangeStr(low * 1000, high * 1000)).rdd.collect()
        print(s"${low}~${high}秒: ")
        println(if (!tmp.nonEmpty) 0 else tmp(0)(0))
      }

      def doPageBasedStatInRange(low: Int, high: Int): Unit = {
        val tmp = sqlContext.sql(sql_pagebase_stat + generateSQLRangeStr(low, high)).rdd.collect()
        println(s"${low}~${high}个页面:(共${tmp.length}个)")
        tmp.foreach(row => println(row(0)))
        println()
      }

      doTimeBasedStatInRange(1, 3)
      doTimeBasedStatInRange(4, 6)
      doTimeBasedStatInRange(7, 9)
      doTimeBasedStatInRange(10, 30)
      doTimeBasedStatInRange(30, 60)

      doPageBasedStatInRange(1, 3)
      doPageBasedStatInRange(4, 6)

    } else if (userInput.getTaskID == "4") {
      // 对通过筛选条件的session，按照各个品类的点击、下单和支付次数，降序排列，获取前10个热门品类
      val limit = try {
        constructSqlLimitHelp(userInput)
      } catch {
        case e: Exception => println(e.getMessage)
          return
      }

      val sql = s"with t as (select * from ${Constants.TABLE_USER_VISIT_ACTION} natural join ${Constants.TABLE_USER_INFO} " +
        s"WHERE date >= '${userInput.getStartDate.getTime}' AND date<= '${userInput.getEndDate.getTime}' " + limit +"), " +
        "click as (select click_product_id ,count(*) as c_1 from t group by click_product_id), " +
        "ord as (select order_product_ids ,count(*) as c_2 from t group by order_product_ids ), " +
        "pay as (select pay_product_ids,count(*) as c_3 from t group by pay_product_ids ) " +
        "select * from click natural join ord natural join pay "
      val rdd = sqlContext.sql(sql).show()


    }
    // 释放资源
    sparkContext.stop()
  }


  def constructSqlLimitHelp(userInput: UserInput): String = {
    // 检查输入合法性
    if (userInput.getStartDate == null) {
      throw new UserInputException("error,parameter startDate is required")
    }
    if (userInput.getEndDate == null) {
      throw new UserInputException("error,parameter startDate is required")
    }
    val professionals = userInput.getProfessionals
    val cities = userInput.getCities
    val searchKeyWords = userInput.getSearchWords
    val clickCategoryIDs = userInput.getClickCategoryIDs

    var limit = ""

    def constructLimitsHelp(colName: String, arr: Array[String]): Unit = {
      if (arr != null && arr.nonEmpty) {
        limit += s" AND $colName in("
        arr.foreach(limit += "'" + _ + "',")
        limit = limit.substring(0, limit.length - 1)
        limit += ") "
      }
    }

    constructLimitsHelp("professional", professionals)
    constructLimitsHelp("city", cities)
    constructLimitsHelp("search_keyword", searchKeyWords)
    constructLimitsHelp("click_category_id", clickCategoryIDs)

    if (userInput.getStartAge != null && userInput.getEndAge != null) {
      limit += s"AND age>'${userInput.getStartAge}' and age<'${userInput.getEndAge}'"
    }
    limit
  }


  //sessionid=value|searchword=value|clickcaterory=value|age=value|professional=value|city=value|sex=value
  def formatToValue(row: Row): String = {
    s"sessionid=${row.get(2)}|searchword=${row.get(5)}|clickcaterory=${row.get(6)}|age=${row.get(16)}|professional=${row.get(17)}|city=${row.get(18)}|sex=${row.get(19)}"
  }

  def getUserInput(dbConnection: Connection): UserInput = {
    //fixme: debug only
    val gson = new Gson()
    val inputJson01 = "{\"taskID\":\"1\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\"}"
    val inputJson02 = "{\"taskID\":\"2\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":16,\"endAge\":40,\"cities\":[\"city6\",\"city48\",\"city77\"]}"
    val inputJson021 ="{\"taskID\":\"4\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":20,\"endAge\":40,\"sex\":\"female\",\"searchWords\":[\"小米5\"],\"cities\":[\"city6\"]}";
    val inputJson03 = "{\"taskID\":\"3\"}"
    val inputJson04 = "{\"taskID\":\"4\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":16,\"endAge\":40,\"cities\":[\"city6\"]}"

    gson.fromJson(inputJson021, classOf[UserInput])
  }
}
