package domain;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by sky on 2017/3/11.
 */
public class UserInput implements Serializable {

    private String taskID;
    private Date startDate;
    private Date endDate;
    private Integer startAge;
    private Integer endAge;
    private String sex;
    private String[] professionals;
    private String[] searchWords;
    private String[] cities;
    private long[] clickCategoryIDs;
    private Timestamp submitTime;

    public UserInput() {
    }

    public Timestamp getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Timestamp submitTime) {
        this.submitTime = submitTime;
    }

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Integer getStartAge() {
        return startAge;
    }

    public void setStartAge(Integer startAge) {
        this.startAge = startAge;
    }

    public Integer getEndAge() {
        return endAge;
    }

    public void setEndAge(Integer endAge) {
        this.endAge = endAge;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String[] getProfessionals() {
        return professionals;
    }

    public void setProfessionals(String[] professionals) {
        this.professionals = professionals;
    }

    public String[] getSearchWords() {
        return searchWords;
    }

    public void setSearchWords(String[] searchWords) {
        this.searchWords = searchWords;
    }

    public String[] getCities() {
        return cities;
    }

    public void setCities(String[] cities) {
        this.cities = cities;
    }

    public long[] getClickCategoryIDs() {
        return clickCategoryIDs;
    }

    public void setClickCategoryIDs(long[] clickCategoryIDs) {
        this.clickCategoryIDs = clickCategoryIDs;
    }
}
