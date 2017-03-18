package session

import domain.SessionRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by sky on 2017/3/17.
  */
object AggregationStatistics {
    def aggreStatics(rdd:RDD[(String, SessionRecord)], sc: SparkContext):String={
        var sessionVisitcount1 = sc.accumulator(0);//累加器1 1~3
        var sessionVisitcount2 = sc.accumulator(0);//累加器2 4~6
        var sessionVisitcount3 = sc.accumulator(0);//累加器3 7~9
        var sessionVisitcount4 = sc.accumulator(0);//累加器4 10~30
        var sessionVisitcount5 = sc.accumulator(0);//累加器5 30~60
        var sessionVisitcount6 = sc.accumulator(0);//累加器5 60以上
        var sessionStepLength1 = sc.accumulator(0);    //步长1~3
        var sessionStepLength2 = sc.accumulator(0);    //步长4~6
        var sessionStepLength3 = sc.accumulator(0);    //步长6以上

        val timerdd = rdd.mapValues(x=>(x.getTimestamps.max)-(x.getTimestamps.min));
        val calculateTime = timerdd.values.foreach(x=>{
            if(x>1&&x<3){sessionVisitcount1+=1;}
            else if(x<6&&x>4){sessionVisitcount2+=1}
            else if(x<9&&x>7){sessionVisitcount3+=1}
            else if(x<30&&x>10){sessionVisitcount4+=1}
            else if (x<60&&x>30){sessionVisitcount5+=1}
            else {sessionVisitcount6+=1}
        })
        val pagerdd = rdd.mapValues(x=>x.getPageRecord.length)
        val calculateStep= pagerdd.values.foreach(x=>{
            if(x>1&&x<3){sessionStepLength1+=1;}
            else if(x<6&&x>4){sessionStepLength2+=1}
            else {sessionStepLength3+=1}
        })
        val result = "session停留时间1~3s"+sessionVisitcount1+"\n4~6"+sessionVisitcount2+"\n7~9"+sessionVisitcount3+"\n10~30"+sessionVisitcount4+"\n30~60"+sessionVisitcount5+"\n超过60"+"\n访问步长1~3"+sessionStepLength1+
                     "\n4~6"+sessionStepLength2+"\n超过6"+sessionStepLength3
        return result
        }
}
