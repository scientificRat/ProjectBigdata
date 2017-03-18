package javautils;
import java.sql.Connection;
import java.sql.SQLException;
import com.mchange.v2.c3p0.*;


/**
 * Created by sky on 2017/3/14.
 */
public class DBHelper {
    static{
        System.setProperty("com.mchange.v2.c3p0.cfg.xml","./src/main/resource/c3p0-config.xml");
    }
    static private ComboPooledDataSource DataSource = new ComboPooledDataSource("stupidRatProj");

    public static Connection getDBConnection() throws SQLException {
        return DataSource.getConnection();
    }

    public static void closeConnection() throws SQLException{
        DataSource.close();
        DataSources.destroy(DataSource);
    }
}

