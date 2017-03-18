package dao;

import constants.Constants;
import domain.TaskRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by sky on 2017/3/15.
 */
public class TaskRecordDAO {
    private Connection cnct;

    public TaskRecordDAO(Connection conn) {
        this.cnct = conn;
    }

    public void insertTaskRecord(TaskRecord tr) throws SQLException{
        PreparedStatement ppstmt = null;

        try{
            ppstmt =  cnct.prepareStatement("INSERT INTO " + Constants.TABLE_TASK_RECORD_INFO + "(category, submitTime, finishTime, record)  VALUES(?, ?, ?, ?)");
            ppstmt.setInt(1, tr.getCategory());
            ppstmt.setTimestamp(2, tr.getSubmitTime());
            ppstmt.setTimestamp(3, tr.getFinishTime());
            ppstmt.setString(4, tr.getResult());

            ppstmt.executeUpdate();
        }
        catch (SQLException sqlerr){
            throw sqlerr;
        }
        finally {
            if (ppstmt != null){
                ppstmt.close();
            }
        }
    }
}
