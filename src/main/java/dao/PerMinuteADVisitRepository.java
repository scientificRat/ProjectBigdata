package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by sky on 2017/3/17.
 */
public class PerMinuteADVisitRepository extends Repository {
    public PerMinuteADVisitRepository(Connection dbConnection) {
        super(dbConnection);
    }

    public void insertOrUpdateOnExist(Date dateOfMinute, String advertisementID, long visitTime) throws SQLException {
        String sql = "INSERT INTO stupidrat.per_minute_ad_visit(date_of_minute, ad_id, visit_time) VALUES (?,?,?) " +
                "ON DUPLICATE KEY UPDATE visit_time = ?";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);

        preparedStatement.setTimestamp(1, new Timestamp(dateOfMinute.getTime()));
        preparedStatement.setString(2, advertisementID);
        preparedStatement.setLong(3, visitTime);
        preparedStatement.setLong(4, visitTime);
        try {
            preparedStatement.execute();
        } catch (SQLException e) {
            // eat up
        }
        preparedStatement.close();
    }
}
