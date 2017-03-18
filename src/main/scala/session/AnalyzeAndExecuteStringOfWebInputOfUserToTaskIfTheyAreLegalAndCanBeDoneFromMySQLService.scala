package session

import javautils.DBHelper

import constants.Constants
import dao.DAOFactory
import domain.{CountRecord, SessionRecord, UserInput}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks
import scalautils.SparkUtils

/**
  * Created by sky on 2017/3/15.
  */
class AnalyzeAndExecuteStringOfWebInputOfUserToTaskIfTheyAreLegalAndCanBeDoneFromMySQLBySessionNotJustActionService
(sparkContext: SparkContext, sqlContext: SQLContext) extends Thread with Serializable{
    override def run(): Unit ={
        // 获得用户输入（输入中ID即为任务类型）
        val dbConnection = DBHelper.getDBConnection

        // 读取数据
        SparkUtils.loadLocalTestDataToTmpTable(sc = sparkContext, sqlContext = sqlContext)

        val loop = new Breaks
        loop.breakable(
            while(true){
                val userInput = DAOFactory.getUIDAO(dbConnection).getUserInput
                if (userInput == null){
                    loop.break
                }

                println(s"Task ${userInput.getTaskID} : ")
                // 开始处理
                userInput.getTaskID match {
                    case "1" => {
                        // 在指定日期范围内，按照session粒度进行数据聚合
                        // check if parameter is legal
                        assert(userInput.getStartDate != null, "error,parameter startDate is required")
                        assert(userInput.getEndDate != null, "error,parameter endDate is required")
                        val rdd = aggregateSessionByDate(sqlContext, userInput.getStartDate.getTime, userInput.getEndDate.getTime)

                        // 输出
                        rdd.collect().foreach(s => println(SessionRecord.toStringOutPut(s._2)))
                    }
                    case "2" => {
                        // 根据用户的查询条件 返回的结果RDD,
                        // 一个或者多个：年龄范围，职业（多选），城市（多选），搜索词（多选），点击品类（多选）进行数据过滤,session时间范围是必选的
                        val rdd = queryRDD(sqlContext, userInput)

                        // 输出
                        rdd.collect().foreach(s => println(SessionRecord.toStringOutPut(s._2)))
                    }
                    case "3" => {
                        val sessionDF = sqlContext.table(s"${Constants.TABLE_USER_VISIT_ACTION}")
                        // join the session and user info
                        val userDF = sqlContext.table(s"${Constants.TABLE_USER_INFO}")
                        val rdd  = sessionDF.join(userDF, Seq("user_id")).rdd

                        println(AggregationStatistics.aggreStatics(
                            rdd.groupBy(_.getString(2)).map(Transformer.rowsToSessionRecord), sparkContext))
                    }
                    case "4" => {
                        // 对通过筛选条件的session，按照各个品类的点击、下单和支付次数，降序排列，获取前10个热门品类
                        val array = getTop10Category(sqlContext, userInput)

                        println("Top 10 Hot Goods : ")
                        array.foreach(s => println(s))
                    }
                }
            }
        )

        // 释放资源
        sparkContext.stop()
        dbConnection.close()
    }

    // Query and aggregate session by time granularity
    def aggregateSessionByDate(sqlContext : SQLContext, beg : Long, end : Long): RDD[(String, SessionRecord)] = {
        var sessionDF = sqlContext.table(s"${Constants.TABLE_USER_VISIT_ACTION}")
        // filter by date limit
        sessionDF = sessionDF.filter(sessionDF("date").gt(beg) &&
            sessionDF("date").lt(end))

        // join the session and user info
        val userDF = sqlContext.table(s"${Constants.TABLE_USER_INFO}")
        val rdd  = sessionDF.join(userDF, Seq("user_id")).rdd

        // aggregate the sessionRecord
        rdd.groupBy(_.getString(2)).map(Transformer.rowsToSessionRecord)
    }

    // query RDD which reach the conditions : age|pro|city|words|click_category|(necessary)time
    def queryRDD(sqlContext: SQLContext, userInput: UserInput): RDD[(String, SessionRecord)] = {
        // check
        assert(userInput.getStartDate != null, "error,parameter startDate is required")
        assert(userInput.getEndDate != null, "error,parameter ebdDate is required")

        var rdd = aggregateSessionByDate(sqlContext, userInput.getStartDate.getTime, userInput.getEndDate.getTime)
        //filter age if required
        if (userInput.getStartAge != null && userInput.getEndAge != null){
            rdd = rdd.filter(kv => {kv._2.getAge >= userInput.getStartAge &&
                kv._2.getAge <= userInput.getEndAge})
        }
        //filter profession if required
        if (userInput.getProfessionals != null){
            rdd = rdd.filter(kv => {userInput.getProfessionals.contains(kv._2.getProfessional)})
        }
        //filter city if required
        if (userInput.getCities != null){
            rdd = rdd.filter(kv => {userInput.getCities.contains(kv._2.getCityName)})
        }
        //filter words if required
        if (userInput.getSearchWords != null){
            rdd = rdd.filter(kv => {userInput.getSearchWords.exists(kv._2.getSearchWord.contains(_))})
        }
        //filter click category if required
        if (userInput.getClickCategoryIDs != null){
            rdd = rdd.filter(kv => {userInput.getClickCategoryIDs.exists( cate =>
                kv._2.getClickRecord.exists(_.id == cate))})
        }
        rdd
    }

    // 对通过筛选条件的session，按照各个品类的点击、下单和支付次数，降序排列，获取前10个热门品类
    def getTop10Category(sqlContext: SQLContext, userInput: UserInput) = {
        val rdd1 = queryRDD(sqlContext, userInput)
        val rdd2 = rdd1.flatMap(Transformer.sessionRecordToCateRec(_)).groupBy(_._1)
            .mapValues(_.reduce((p1, p2) => (p1._1, p1._2.add(p2._2)))._2)
        val rdd3 = rdd2.map(_._2).sortBy(p => p)

        rdd3.take(10)
    }
}