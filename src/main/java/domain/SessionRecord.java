package domain;

import java.io.Serializable;
import java.sql.Timestamp;
import java.sql.Date;

/**
 * Created by sky on 2017/3/15.
 */
public class SessionRecord implements Serializable {
    public static String toStringOutPut(SessionRecord sessionRecord){
        return "sessionid="+sessionRecord.getSessionID()+"|searchword="+sessionRecord.getSearchWord()+"|clickcategrory="+sessionRecord.getClickRecord()+"|age="+sessionRecord.age+"|professional="+sessionRecord.getProfessional()+
                "|city="+sessionRecord.getCityName()+"|sex="+sessionRecord.getSex();
    }

    static public class Product implements Serializable{
        public long id;
        public long category;

        public Product(long id, long category){
            this.id = id;
            this.category = category;
        }
    }

    private String sessionID;
    private long date;
    private long userID;
    private long[] pageRecord;
    private long[] timestamps;
    private String[] searchWord;
    private Product[] clickRecord;
    private Product[] orderRecord;
    private Product[] payRecord;
    private long cityID;
    private String userName;
    private String sex;
    private String cityName;
    private String name;
    private int age;
    private String professional;

    public SessionRecord() {}

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getProfessional() {
        return professional;
    }

    public void setProfessional(String professional) {
        this.professional = professional;
    }

    public String getSessionID() {
        return sessionID;
    }

    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public long getUserID() {
        return userID;
    }

    public void setUserID(long userID) {
        this.userID = userID;
    }

    public long[] getPageRecord() {
        return pageRecord;
    }

    public void setPageRecord(long[] pageRecord) {
        this.pageRecord = pageRecord;
    }

    public long[] getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(long[] timestamps) {
        this.timestamps = timestamps;
    }

    public String[] getSearchWord() {
        return searchWord;
    }

    public void setSearchWord(String[] searchWord) {
        this.searchWord = searchWord;
    }

    public Product[] getClickRecord() {
        return clickRecord;
    }

    public void setClickRecord(Product[] clickRecord) {
        this.clickRecord = clickRecord;
    }

    public Product[] getOrderRecord() {
        return orderRecord;
    }

    public void setOrderRecord(Product[] orderRecord) {
        this.orderRecord = orderRecord;
    }

    public Product[] getPayRecord() {
        return payRecord;
    }

    public void setPayRecord(Product[] payRecord) {
        this.payRecord = payRecord;
    }

    public long getCityID() {
        return cityID;
    }

    public void setCityID(long cityID) {
        this.cityID = cityID;
    }

    @Override
    public String toString() {
        String str = sessionID;
        str += "|" + new Date(date).toString() + "|" + userID + "|" + cityID + "|" + userName + "|" +
                sex + "|" + cityName + "|" + name + "|" + age + "|" + professional;

        String page = "\nPageRecord : [", time = "\nTime : [";
        for (int i = 0; i < pageRecord.length; ++i){
            page += pageRecord[i] + ",";
            time += new Timestamp(timestamps[i]).toString() + ",";
        }
        str += page + "]" + time + "]";

        str += "\nSearchWord : [";
        for (String word : searchWord){
            str += word + ",";
        }
        str += ']';

        str += "\nClickRecord : [";
        for (int i = 0; i < clickRecord.length; ++i){
            str += clickRecord[i].category + "." + clickRecord[i].id + "|";
        }
        str += ']';

        str += "\nOrderRecord : [";
        for (int i = 0; i < orderRecord.length; ++i){
            str += orderRecord[i].category + "." + orderRecord[i].id + "|";
        }
        str += ']';

        str += "\nPayRecord : [";
        for (int i = 0; i < payRecord.length; ++i){
            str += payRecord[i].category + "." + payRecord[i].id + "|";
        }
        str += ']';

        return str;
    }
}
