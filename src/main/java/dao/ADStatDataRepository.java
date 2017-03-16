package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by sky on 2017/3/16.
 */
public class ADStatDataRepository extends Repository {

    public ADStatDataRepository(Connection dbConnection) {
        super(dbConnection);
    }

    public void insertOrUpdateOnExist(String dateOfDay, String province, String city, String advertisementID, long visitTime) throws SQLException {
        String sql = "INSERT INTO stupidrat.ad_statistic_data(date_of_day, province, city, ad_id) VALUES (?,?,?,?) " +
                "ON DUPLICATE KEY UPDATE visit_time = ?";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setString(1, dateOfDay);
        preparedStatement.setString(2, province);
        preparedStatement.setString(3, city);
        preparedStatement.setString(4, advertisementID);
        preparedStatement.setLong(5, visitTime);
        preparedStatement.execute();
        preparedStatement.close();
    }

    public String queryEveryDayTop3OfProvince() throws SQLException {
        String sql = "SELECT ad_id FROM stupidrat.ad_statistic_data WHERE ad_id  ";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);


        preparedStatement.close();
        return null;
    }
}
