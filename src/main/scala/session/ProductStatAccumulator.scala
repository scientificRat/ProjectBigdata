package session

import domain.{ProductStat, SessionRowProduct}
import org.apache.spark.{AccumulableParam, AccumulatorParam}

import scala.collection.mutable

/**
  * Created by sky on 2017/3/11.
  */
object ProductStatAccumulator extends AccumulableParam[mutable.Map[Long, ProductStat], SessionRowProduct] {
  override def addAccumulator(r: mutable.Map[Long, ProductStat], t: SessionRowProduct): mutable.Map[Long, ProductStat] = {
    if (t.hasClickProductID) {
      if (r.contains(t.getClickProductID)) {
        r(t.getClickProductID).addClickTime()
      } else {
        r += (t.getClickProductID -> new ProductStat().addClickTime())
      }
    }
    if (t.hasOrderProductID) {
      if (r.contains(t.getOrderProductID)) {
        r(t.getOrderProductID).addOrderTime()
      } else {
        r += (t.getOrderProductID -> new ProductStat().addOrderTime())
      }

    }
    if (t.hasPayProductID) {
      if (r.contains(t.getPayProductID)) {
        r(t.getPayProductID).addPayTime()
      } else {
        r += (t.getPayProductID -> new ProductStat().addPayTime())
      }
    }
    r
  }

  override def addInPlace(r1: mutable.Map[Long, ProductStat], r2: mutable.Map[Long, ProductStat]): mutable.Map[Long, ProductStat] = {
    r2.foreach(tp => {
      if (r1.contains(tp._1)) {
        r1(tp._1).add(tp._2)
      }
      else {
        r1 += tp
      }
    })
    r1
  }

  override def zero(initialValue: mutable.Map[Long, ProductStat]): mutable.Map[Long, ProductStat] = {
    initialValue
  }
}
