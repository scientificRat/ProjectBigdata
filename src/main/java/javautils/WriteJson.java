package javautils; /**
 * Created by sky on 2017/3/17.
 */
import org.apache.commons.net.ntp.TimeStamp;

import javax.swing.plaf.nimbus.State;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;

public class WriteJson {
    static String inputJson01 = "{\"taskID\":\"1\",\"startDate\":\"2017-03-05\",\"endDate\":\"2017-04-06\"}";
    static String inputJson02 = "{\"taskID\":\"2\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":16,\"endAge\":40,\"cities\":[\"city6\",\"city48\",\"city77\"]}";
    static String inputJson021 = "{\"taskID\":\"4\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":20,\"endAge\":40,\"sex\":\"female\",\"searchWords\":[\"小米5\"],\"cities\":[\"city6\"]}";
    static String inputJson03 = "{\"taskID\":\"3\"}";
    static String inputJson04 = "{\"taskID\":\"4\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":16,\"endAge\":40,\"cities\":[\"city6\"]}";
    static String inputJson044 = "{\"taskID\":\"4\",\"startDate\":\"2017-01-06\",\"endDate\":\"2017-04-06\",\"startAge\":16,\"endAge\":40}";

    public static void main(String args[]){
        String url = "jdbc:mysql://192.168.2.42:3306/stupidrat";
        String user = "root";
        String password = "123456";
        String sql;
        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        Connection conn = null;

        try {
           Date date = new Date();
           Timestamp nousedate = new Timestamp(date.getTime());
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url,user,password);
            Statement statement = conn.createStatement();
            sql = "INSERT INTO idcproj (JSON,submit_time) VALUES('"+inputJson01+"','"+nousedate+"')";
            int rs1 = statement.executeUpdate(sql);
            sql = "INSERT INTO idcproj (JSON,submit_time) VALUES('"+inputJson02+"','"+nousedate+"')";
            int rs2 = statement.executeUpdate(sql);
            sql = "INSERT INTO idcproj (JSON,submit_time) VALUES('"+inputJson021+"','"+nousedate+"')";
            int rs3 = statement.executeUpdate(sql);
            sql = "INSERT INTO idcproj (JSON,submit_time) VALUES('"+inputJson03+"','"+nousedate+"')";
            int rs4 = statement.executeUpdate(sql);
            sql = "INSERT INTO idcproj (JSON,submit_time) VALUES('"+inputJson04+"','"+nousedate+"')";
            int rs5 = statement.executeUpdate(sql);
            sql = "INSERT INTO idcproj (JSON,submit_time) VALUES('"+inputJson044+"','"+nousedate+"')";
            int rs6 = statement.executeUpdate(sql);

            conn.close();
        }catch (Exception   e){
            e.printStackTrace();
        }
    }
}
