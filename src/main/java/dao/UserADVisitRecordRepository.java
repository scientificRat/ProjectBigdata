package dao;

import domain.MayNoneInteger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by sky on 2017/3/15.
 */
public class UserADVisitRecordRepository extends Repository {
    public UserADVisitRecordRepository(Connection dbConnection) {
        super(dbConnection);
    }

    public void insertOrUpdateOnExist(String dateOfDay, String userID, String advertisementID, long visitTime) throws SQLException {
        String sql = "INSERT INTO stupidrat.user_ad_visit_record(user_id,ad_id,date_of_day,visit_time) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE visit_time = visit_time + ? ";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setString(1, userID);
        preparedStatement.setString(2, advertisementID);
        preparedStatement.setString(3, dateOfDay);
        preparedStatement.setLong(4, visitTime);
        preparedStatement.setLong(5, visitTime);
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

    public String[] queryBlackList() throws SQLException {
        String sql = "SELECT user_id FROM stupidrat.user_ad_visit_record WHERE visit_time >=100 ";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        ArrayList<String> blackList = new ArrayList<>();
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            blackList.add(resultSet.getString(1));
        }
        preparedStatement.close();
        return blackList.toArray(new String[0]);
    }
}
