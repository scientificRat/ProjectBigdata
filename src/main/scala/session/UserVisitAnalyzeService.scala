package session

import java.sql.Connection
import javautils.DBHelper

import com.google.gson.Gson
import constants.Constants
import domain.{CountRecord, UserInput}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scalautils.SparkUtils

/**
  * Created by sky on 2017/3/11.
  */
object UserVisitAnalyzeService {
    def main(args: Array[String]): Unit = {
        //TA: 不要把所有逻辑写在main里
        //TA: 傻逼，不是这意思
        val sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster("local[4]")
        // 负责和集群通信
        val sparkContext = new SparkContext(sparkConf)
        // spark sql是建立在sparkCores上面的，那么自然而然需要使用到sparkContext进行通信
        val sqlContext = new SQLContext(sparkContext)

        new AnalyzeAndExecuteStringOfWebInputOfUserToTaskIfTheyAreLegalAndCanBeDoneFromMySQLBySessionNotJustActionService(sparkContext, sqlContext).start()
        //isNotMain(args)
    }

    def isNotMain(args: Array[String]): Unit = {
        // 配置Spark
        val sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster("local[4]")
        // 负责和集群通信
        val sparkContext = new SparkContext(sparkConf)
        // spark sql是建立在sparkCores上面的，那么自然而然需要使用到sparkContext进行通信
        val sqlContext = new SQLContext(sparkContext)

        // 获取输入
        val dbConnection = DBHelper.getDBConnection
        val userInput = getUserInput(dbConnection)

        // 读取数据
        SparkUtils.loadLocalTestDataToTmpTable(sc = sparkContext, sqlContext = sqlContext)
        // 开始处理
        userInput.getTaskID match {
            case "1" => {
//                sqlContext.sql(s"select * from ${Constants.TABLE_USER_VISIT_ACTION}").show()
                // 在指定日期范围内，按照session粒度进行数据聚合
                val rdd = aggregateSessionInDateRange(sqlContext, userInput)
//                // 输出
                convertToFormattedRowOutput(rdd).foreach(println)
            }
            case "2" => {
                // 根据用户的查询条件 返回的结果RDD,
                // 一个或者多个：年龄范围，职业（多选），城市（多选），搜索词（多选），点击品类（多选）进行数据过滤,session时间范围是必选的
                val rdd = filterRDD(sqlContext, userInput)
                // 输出
                convertToFormattedRowOutput(rdd).foreach(println)
            }
            case "3" => {
                // fixme: 因为前任智力有问题，这里需要封装和优化
                // 实现自定义累加器完成多个聚合统计业务的计算,
                // 统计业务包括访问时长：1~3秒，4~6秒，7~9秒，10~30秒，30~60秒的session访问量统计，
                // 访问步长：1~3个页面，4~6个页面等步长的访问统计
                var sql_timebase_stat = s"select session_id from ${Constants.TABLE_USER_VISIT_ACTION} " +
                    s"group by session_id having (max(action_time)-min(action_time)) "
                var sql_pagebase_stat = s"select session_id from ${Constants.TABLE_USER_VISIT_ACTION} " +
                    s"group by session_id having count(*) "

                def generateSQLRangeStr(low: Int, high: Int): String = {
                    s"between $low and $high"
                }

                def doTimeBasedStatInRange(low: Int, high: Int): Unit = {
                    // 1000表示从 ms --> s
                    val tmp = sqlContext.sql(sql_timebase_stat + generateSQLRangeStr(low * 1000, high * 1000))
                        .rdd.collect()
                    println(s"${low}~${high}秒: (共${tmp.length}个) ")
                    tmp.foreach(row => println(row(0)))
                }

                def doPageBasedStatInRange(low: Int, high: Int): Unit = {
                    val tmp = sqlContext.sql(sql_pagebase_stat + generateSQLRangeStr(low, high))
                        .rdd.collect()
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

            }
            case "4" => {
            }
        }

        // 释放资源
        sparkContext.stop()
        dbConnection.close()
    }

    // 将查询输出转化为标准化的形式
    def convertToFormattedRowOutput(rdd: RDD[Row]): RDD[(String, String)] = {
        rdd.map(row => {
            (row.getString(2), formatToValue(row))
        })
    }

    // 在指定日期范围内，按照session粒度进行数据聚合
    def aggregateSessionInDateRange(sqlContext: SQLContext, userInput: UserInput): RDD[Row] = {
        // 检查输入合法性
        assert(userInput.getStartDate != null, "error,parameter startDate is required")
        assert(userInput.getEndDate != null, "error,parameter endDate is required")

        // 查询
        val sql =s"select * from ${Constants.TABLE_USER_VISIT_ACTION} as t1, " +
            s"${Constants.TABLE_USER_INFO} as t2 " +
            s"WHERE t1.user_id = t2.user_id " +
            s"AND date >= '${userInput.getStartDate.getTime}' AND date< '${userInput.getEndDate.getTime}'"
        println(sql)
        sqlContext.sql(sql).rdd
    }

    // 根据用户的查询条件 返回的结果RDD,
    // 一个或者多个：年龄范围，职业（多选），城市（多选），搜索词（多选），点击品类（多选）进行数据过滤,session时间范围是必选的
    def filterRDD(sqlContext: SQLContext, userInput: UserInput): RDD[Row] = {
        // 检查输入合法性
        assert(userInput.getStartDate != null, "error,parameter startDate is required")
        assert(userInput.getEndDate != null, "error,parameter ebdDate is required")
        val limit = constructSqlLimitHelp(userInput)
        val sql = s"select * from ${Constants.TABLE_USER_VISIT_ACTION} as t1, ${Constants.TABLE_USER_INFO} as t2 " +
            s"WHERE t1.user_id = t2.user_id " +
            s"AND date >= '${userInput.getStartDate.getTime}' AND date< '${userInput.getEndDate.getTime}' " + limit

        sqlContext.sql(sql).rdd
    }


    private def constructSqlLimitHelp(userInput: UserInput): String = {
        null
    }


    //sessionid=value|searchword=value|clickcaterory=value|age=value|professional=value|city=value|sex=value
    private def formatToValue(row: Row): String = {
        s"sessionid=${row.get(2)}|searchword=${row.get(5)}|clickcaterory=${row.get(6)}|age=${row.get(16)}|" +
            s"professional=${row.get(17)}|city=${row.get(18)}|sex=${row.get(19)}"
    }

    private def getUserInput(dbConnection: Connection): UserInput = {
        //fixme: debug only
        val gson = new Gson()
        val stmt = dbConnection.createStatement()

        //        val inputJson01 = "{\"taskID\":\"1\",\"startDate\":\"2017-03-05\",\"endDate\":\"2017-04-06\"}"
        //        val inputJson02 = "{\"taskID\":\"2\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":16,\"endAge\":40,\"cities\":[\"city6\",\"city48\",\"city77\"]}"
        //        val inputJson021 = "{\"taskID\":\"4\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":20,\"endAge\":40,\"sex\":\"female\",\"searchWords\":[\"小米5\"],\"cities\":[\"city6\"]}";
        //        val inputJson03 = "{\"taskID\":\"3\"}"
        //        val inputJson04 = "{\"taskID\":\"4\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":16,\"endAge\":40,\"cities\":[\"city6\"]}"
        //        val inputJson044 = "{\"taskID\":\"4\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":16,\"endAge\":40}"

        val res = stmt.executeQuery("SELECT * FROM IDCproj ORDER BY ID LIMIT 1")
        val input = if (res.next()) gson.fromJson(res.getString("JSON"), classOf[UserInput])
        else null

        stmt.close()
        input
    }
}
