package domain;

import java.io.Serializable;

/**
 * Created by sky on 2017/3/15.
 */
public class MayNoneInteger implements Serializable {

    private int value = 0;
    private boolean none = true;

    public MayNoneInteger() {
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.none = false;
        this.value = value;
    }

    public boolean isNone() {
        return none;
    }

    public void setNone(boolean none) {
        this.none = none;
    }
}
