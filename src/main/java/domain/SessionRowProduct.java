package domain;

/**
 * Created by sky on 2017/3/12.
 */
public class SessionRowProduct {
    private long clickProductID = -1;
    private long orderProductID = -1;
    private long payProductID = -1;

    public boolean hasClickProductID() {
        return clickProductID != -1;
    }

    public boolean hasOrderProductID() {
        return orderProductID != -1;
    }

    public boolean hasPayProductID() {
        return payProductID != -1;
    }

    public long getClickProductID() {
        return clickProductID;
    }

    public void setClickProductID(long clickProductID) {
        this.clickProductID = clickProductID;
    }

    public long getOrderProductID() {
        return orderProductID;
    }

    public void setOrderProductID(long orderProductID) {
        this.orderProductID = orderProductID;
    }

    public long getPayProductID() {
        return payProductID;
    }

    public void setPayProductID(long payProductID) {
        this.payProductID = payProductID;
    }
}
