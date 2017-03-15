package domain;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by sky on 2017/3/15.
 */
public class ADVisitRecord implements Serializable {

    private Date date;
    private String province;
    private String city;
    private String userID;
    private String advertisementID;

    public ADVisitRecord() {
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
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
}
