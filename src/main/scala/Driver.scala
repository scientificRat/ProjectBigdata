import constants.Constants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import service.RealTimeADStatService
import session.AnalyzeAndExecuteStringOfWebInputOfUserToTaskIfTheyAreLegalAndCanBeDoneFromMySQLBySessionNotJustActionService

/**
  * Created by sky on 2017/3/15.
  */
object Driver {

    def main(args: Array[String]): Unit = {
        // 配置Spark
        val sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster("local[4]")
        // 负责和集群通信
        val sparkContext = new SparkContext(sparkConf)
        // spark sql是建立在sparkCores上面的，那么自然而然需要使用到sparkContext进行通信
        val sqlContext = new SQLContext(sparkContext)

        // 开启广告流量统计任务
        val realTimeADStatService = new RealTimeADStatService(sparkContext)
        realTimeADStatService.start()
        println("###--adStat----start--###")

        // 开启用户访问分析任务
        val analyzeAndExecuteStringOfWebInputOfUserToTaskIfTheyAreLegalAndCanBeDoneFromMySQLBySessionNotJustActionService =
            new AnalyzeAndExecuteStringOfWebInputOfUserToTaskIfTheyAreLegalAndCanBeDoneFromMySQLBySessionNotJustActionService(sparkContext, sqlContext)
        analyzeAndExecuteStringOfWebInputOfUserToTaskIfTheyAreLegalAndCanBeDoneFromMySQLBySessionNotJustActionService.start
        println("###--userVisitAnalyze-----start--###")

        // 等待线程结束
        realTimeADStatService.join()
        println("--adStat-----end")

        // 等待线程结束
//        userVisitAnalyzeService.join()
        println("--userVisitAnalyze-----end")

        // 释放资源
        sparkContext.stop()
        println("\nBye")
    }
}
