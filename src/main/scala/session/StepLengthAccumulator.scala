package session

import org.apache.spark.AccumulatorParam

/**
  * Created by sky on 2017/3/18.
  */
object StepLengthAccumulator extends AccumulatorParam[String]{
    var sla1 = 0L
    var sla2 = 0L
    var sla3 = 0L

    var result = "1~3=0|4~6=0|6 and more = 0"

    override def addInPlace(r1: String, r2: String): String = {
        val  steplength = r2.toLong
        if(steplength>1&&steplength<3){sla1+=1}
        else if(steplength<6&&steplength>4){sla2+=1}
        else {sla3+=1}
        result = "1~3="+sla1+"|4~6="+sla2+"|6 and more = "+sla3;
        return result
    }

    override def zero(initialValue: String): String = {
        result = "1~3=0|4~6=0|6 and more = 0"
        return result
    }

}
