package domain;

import java.io.Serializable;

/**
 * Created by sky on 2017/3/16.
 */
public class RawRealTimeAd implements Serializable {
    private String dateOfDay;
    private String province;
    private String city;
    private String userID;
    private String advertisementID;


    public RawRealTimeAd() {
    }

    public RawRealTimeAd(String dateOfDay, String province, String city, String userID, String advertisementID) {
        this.dateOfDay = dateOfDay;
        this.province = province;
        this.city = city;
        this.userID = userID;
        this.advertisementID = advertisementID;
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

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
