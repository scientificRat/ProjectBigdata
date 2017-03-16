package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by sky on 2017/3/16.
 */
public class EverydayTop3ADRepository extends Repository {

    public EverydayTop3ADRepository(Connection dbConnection) {
        super(dbConnection);
    }

    public void insertOrUpdateOnExist(String dateOfDay, String province, String firstAD, String secondAD, String thirdAD) throws SQLException {
        String sql = "INSERT INTO stupidrat.everyday_top3_ad_of_province(date_of_day, province, first_ad_id, second_ad_id, third_ad_id) VALUES (?,?,?,?,?) " +
                "ON DUPLICATE KEY UPDATE first_ad_id= ?, second_ad_id = ? , third_ad_id = ?";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);

        preparedStatement.setString(1, dateOfDay);
        preparedStatement.setString(2, province);
        preparedStatement.setString(3, firstAD);
        preparedStatement.setString(4, secondAD);
        preparedStatement.setString(5, thirdAD);
        preparedStatement.setString(6, firstAD);
        preparedStatement.setString(7, secondAD);
        preparedStatement.setString(8, thirdAD);

        preparedStatement.execute();
        preparedStatement.close();
    }
}
