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

    public void doJob() throws SQLException {
        String sql =
                "REPLACE INTO everyday_top3_ad_of_province (date_of_day, province, top3_ad)" +
                "  SELECT date_of_day,province," +
                "    GROUP_CONCAT(CONCAT('(', ad_id, ',', visit_time, ')') ORDER BY visit_time DESC SEPARATOR ',') AS top3_ad" +
                "  FROM ad_statistic_data AS t1" +
                "  WHERE 3 > (SELECT count(*)" +
                "             FROM ad_statistic_data AS t2" +
                "             WHERE t1.date_of_day = t2.date_of_day AND t1.province = t2.province AND t2.visit_time > t1.visit_time)" +
                "  GROUP BY date_of_day, province";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.execute();
        preparedStatement.close();
    }
}
