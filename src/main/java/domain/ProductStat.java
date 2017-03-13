package domain;

import java.io.Serializable;

/**
 * Created by sky on 2017/3/11.
 */
public class ProductStat implements Comparable<ProductStat>, Serializable {

    private long clickTime = 0;
    private long orderTime = 0;
    private long payTime = 0;


    public ProductStat(long clickTime, long orderTime, long payTime) {
        this.clickTime = clickTime;
        this.orderTime = orderTime;
        this.payTime = payTime;
    }

    public ProductStat() {
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

    public ProductStat addClickTime() {
        clickTime++;
        return this;
    }

    public ProductStat addOrderTime() {
        orderTime++;
        return this;
    }

    public ProductStat addPayTime() {
        payTime++;
        return this;
    }

    public ProductStat add(ProductStat o) {
        this.clickTime += o.clickTime;
        this.orderTime += o.orderTime;
        this.payTime += o.payTime;
        return this;
    }


    @Override
    public int compareTo(ProductStat o) {
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
        return clickTime + " | " + orderTime + " | " + payTime;
    }
}
