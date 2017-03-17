package domain;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by sky on 2017/3/15.
 */
public class TaskRecord implements Serializable {
    private int category;
    private Timestamp finishTime;

    public Timestamp getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Timestamp submitTime) {
        this.submitTime = submitTime;
    }

    private Timestamp submitTime;
    private String result;

    public TaskRecord() {
    }

    public int getCategory() {
        return category;
    }

    public void setCategory(int category) {
        this.category = category;
    }

    public Timestamp getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(Timestamp finishTime) {
        this.finishTime = finishTime;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
