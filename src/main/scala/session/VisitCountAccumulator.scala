package session

import org.apache.spark.AccumulatorParam
import org.slf4j.LoggerFactory

/**
  * Created by sky on 2017/3/18.
  */
object VisitCountAccumulator extends AccumulatorParam[String]{
    var asc1 = 0L
    var asc2 = 0L
    var asc3 = 0L
    var asc4 = 0L
    var asc5 = 0L
    var asc6 = 0L
    var result = "1~3=0|4~6=0|7~9=0|10~30=0|30~60=0|60 and more = 0"

    override def addInPlace(r1: String, r2: String): String = {
        val  time = r2.toLong
        if(time>1&&time<3){asc1+=1}
        else if(time<6&&time>4){asc2+=1}
        else if(time<9&&time>7){asc3+=1}
        else if(time<30&&time>10){asc4+=1}
        else if (time<60&&time>30){asc5+=1}
        else {asc6+=1}
        result = "1~3="+asc1+"|4~6="+asc2+"|7~9="+asc3+"|10~30="+asc4+"|30~60="+asc5+"|60 and more = "+asc6
        return result
    }

    override def zero(initialValue: String): String = {
        result = null;
        return result
    }

}
