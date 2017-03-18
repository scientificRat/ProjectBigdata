package domain;

import java.io.Serializable;

/**
 * Created by sky on 2017/3/15.
 */
public class UserADVisitRecord implements Serializable {

    private String dateOfDay;
    private String userID;
    private String advertisementID;
    private long visitTime = 0;

    public UserADVisitRecord(String dateOfDay, String userID, String advertisementID, long visitTime) {
        this.dateOfDay = dateOfDay;
        this.userID = userID;
        this.advertisementID = advertisementID;
        this.visitTime = visitTime;
    }

    public UserADVisitRecord() {
    }

    public String getDateOfDay() {
        return dateOfDay;
    }

    public void setDateOfDay(String dateOfDay) {
        this.dateOfDay = dateOfDay;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getAdvertisementID() {
        return advertisementID;
    }

    public void setAdvertisementID(String advertisementID) {
        this.advertisementID = advertisementID;
    }

    public long getVisitTime() {
        return visitTime;
    }

    public void setVisitTime(long visitTime) {
        this.visitTime = visitTime;
    }
}
