package domain;

import java.io.Serializable;

/**
 * Created by sky on 2017/3/11.
 */
public class CountRecord implements Comparable<CountRecord>, Serializable {
    private long clickTime = 0;
    private long orderTime = 0;
    private long payTime = 0;
    private long categoryID = 0;

    public CountRecord(long clickTime, long orderTime, long payTime, long categoryID) {
        this.clickTime = clickTime;
        this.orderTime = orderTime;
        this.payTime = payTime;
        this.categoryID = categoryID;
    }

    public CountRecord() {
    }

    public long getCategoryID() {
        return categoryID;
    }

    public long getClickTime() {
        return clickTime;
    }

    public long getOrderTime() {
        return orderTime;
    }

    public long getPayTime() {
        return payTime;
    }

    public CountRecord addTime(int order){
        switch (order){
            case 1:addClickTime();break;
            case 2:addOrderTime();break;
            case 3:addPayTime();break;
        }
        return this;
    }

    public CountRecord addClickTime() {
        clickTime++;
        return this;
    }

    public CountRecord addOrderTime() {
        orderTime++;
        return this;
    }

    public CountRecord addPayTime() {
        payTime++;
        return this;
    }

    public CountRecord add(CountRecord o) {
        this.clickTime += o.clickTime;
        this.orderTime += o.orderTime;
        this.payTime += o.payTime;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CountRecord){
            CountRecord cr = (CountRecord)obj;
            if (cr.clickTime == clickTime &&
                    cr.categoryID == categoryID &&
                    cr.orderTime == orderTime &&
                    cr.payTime == payTime){
                return true;
            }
        }
        return false;
    }

    @Override
    public int compareTo(CountRecord o) {
        if (clickTime == o.clickTime) {
            if (orderTime == o.orderTime) {
                return (int) (payTime - o.payTime);
            }
            return (int) (orderTime - o.orderTime);
        }
        return (int) (clickTime - o.clickTime);
    }

    @Override
    public String toString() {
        return categoryID + " : " +  clickTime + " | " + orderTime + " | " + payTime;
    }
}
