package dao;

import domain.MayNoneInteger;
import javautils.DBHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by sky on 2017/3/15.
 */
public class UserADVisitRecordRepository {
    Connection dbConnection = null;

    public UserADVisitRecordRepository() {
        dbConnection = DBHelper.getDBConnection();
    }

    public MayNoneInteger queryVisitTime(String userID, String advertisementID, String dateOfDay) throws SQLException {
        String sql = "SELECT visit_time FROM user_ad_visit_record WHERE date_of_day=? AND user_id=? AND ad_id=?";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setString(1, dateOfDay);
        preparedStatement.setString(2, userID);
        preparedStatement.setString(3, advertisementID);
        ResultSet resultSet = preparedStatement.executeQuery();
        MayNoneInteger rst = new MayNoneInteger();
        if (resultSet.next()) {
            rst.setValue(resultSet.getInt("visit_time"));
        }
        preparedStatement.close();
        return rst;
    }

    public void updateVisitTime(String userID, String advertisementID, String dateOfDay, long newVisitTime) throws SQLException {
        String sql = "UPDATE user_ad_visit_record SET visit_time = ? WHERE date_of_day=? AND user_id=? AND ad_id=?";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setLong(1, newVisitTime);
        preparedStatement.setString(2, dateOfDay);
        preparedStatement.setString(3, userID);
        preparedStatement.setString(4, advertisementID);
        preparedStatement.execute();
        preparedStatement.close();

    }

    public void add(String userID, String advertisementID, String dateOfDay, long visitTime) throws SQLException {
        String sql = "INSERT INTO user_ad_visit_record(user_id,ad_id,date_of_day,visit_time) VALUES (?,?,?,?)";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setString(1, userID);
        preparedStatement.setString(2, advertisementID);
        preparedStatement.setString(3, dateOfDay);
        preparedStatement.setLong(4, visitTime);
        preparedStatement.execute();
        preparedStatement.close();

    }

    public boolean queryIsBlack(String dateOfDay, String userID, String advertisementID) throws SQLException {
        String sql = "SELECT user_id FROM user_ad_visit_record " +
                "WHERE visit_time>100 AND date_of_day=? AND user_id=? AND ad_id=?";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setString(1, dateOfDay);
        preparedStatement.setString(2, userID);
        preparedStatement.setString(3, advertisementID);
        ResultSet resultSet = preparedStatement.executeQuery();
        boolean rst = false;
        if (resultSet.next()) {
            rst = true;
        }
        preparedStatement.close();
        return rst;
    }

}
