package javautils;

import com.mysql.jdbc.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by sky on 2017/3/14.
 */
public class DBHelper {

    //default database
    private static final String DATABASE_URL = "jdbc:mysql://192.168.2.3/stupidRat";
    private static final String DATABASE_USER_NAME = "root";
    private static final String DATABASE_USER_PASSWORD = "123456";

    //default
    public static Connection getDBConnection() {
        return getDBConnection(DATABASE_URL, DATABASE_USER_NAME, DATABASE_USER_PASSWORD);
    }

    public static Connection getDBConnection(String databaseUrl, String databaseUserName, String databaseUserPassword) {
        Connection dbConnection = null;
        try {
            Driver driver = new Driver();
            DriverManager.registerDriver(driver);
            dbConnection = DriverManager.getConnection(databaseUrl, databaseUserName, databaseUserPassword);
        } catch (SQLException e) {
            for (Throwable t : e) {
                t.printStackTrace();
            }
        }
        return dbConnection;
    }

    // for debug
    public static void main(String[] args) {
        Connection connection = getDBConnection();

        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}

