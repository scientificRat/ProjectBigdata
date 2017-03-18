package domain;

import java.io.Serializable;

/**
 * Created by huangzhengyue on 2017/3/18.
 */
public class LongRange implements Serializable {
    private long low;
    private long high;

    public LongRange() {
    }

    public LongRange(long low, long high) {
        this.low = low;
        this.high = high;
    }

    public long getLow() {
        return low;
    }

    public void setLow(int low) {
        this.low = low;
    }

    public long getHigh() {
        return high;
    }

    public void setHigh(int high) {
        this.high = high;
    }

    public boolean contains(long v) {
        return v >= low && v < high;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LongRange) {
            LongRange r = (LongRange) obj;
            if (r.getHigh() == high && r.getLow() == low) {
                return true;
            }
        }
        return false;
    }

    // not good enough
    @Override
    public int hashCode() {
        return (int) (low + high);
    }
}
