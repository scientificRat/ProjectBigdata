package domain;

/**
 * Created by sky on 2017/3/11.
 */
public class ProductStat implements Comparable<ProductStat> {

    private long clickTime = 0;
    private long orderTime = 0;
    private long payTime = 0;

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


    @Override
    public int compareTo(ProductStat o) {
        if(clickTime==o.clickTime){
            if(orderTime == o.orderTime){
                return (int) (payTime - o.payTime);
            }
            return (int) (orderTime-o.orderTime);
        }
        return (int) (clickTime-o.clickTime);
    }
}
