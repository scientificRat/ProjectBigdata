package domain;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by sky on 2017/3/16.
 */
public class RawRealTimeAd implements Serializable {

    private long date;
    private String province;
    private String city;
    private String userID;
    private String advertisementID;


    public RawRealTimeAd() {
    }

    public RawRealTimeAd(long date, String province, String city, String userID, String advertisementID) {
        this.date = date;
        this.province = province;
        this.city = city;
        this.userID = userID;
        this.advertisementID = advertisementID;
    }

    public String getDateOfDayStr() {
        return new SimpleDateFormat("yyyy-MM-dd").format(date);
    }

    public Date getDateOfMinute(){
        return new Date(getMinute()*60*1000);
    }

    public long getDate() {
        return date;
    }

    public long getMinute() {
        return date / (60 * 1000);
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
