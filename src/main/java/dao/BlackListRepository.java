package dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by sky on 2017/3/17.
 */
public class BlackListRepository extends Repository {
    public BlackListRepository(Connection dbConnection) {
        super(dbConnection);
    }

    public void insertOrIgnoreOnExist(String dateOfDay, String userID) throws SQLException {
        String sql = "INSERT IGNORE INTO stupidrat.black_list(dateOfDay, user_id) VALUES (?,?)";
        PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
        preparedStatement.setString(1, dateOfDay);
        preparedStatement.setString(2, userID);
        preparedStatement.execute();
        preparedStatement.close();
    }

    public String[] queryBlackList() throws SQLException {
        String sql = "SELECT user_id FROM stupidrat.black_list";
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
