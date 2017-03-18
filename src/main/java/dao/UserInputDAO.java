package dao;

import com.google.gson.Gson;
import constants.Constants;
import domain.UserInput;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by sky on 2017/3/15.
 */
public class UserInputDAO {
    private Connection cnct;

    public UserInputDAO(Connection cnct) {
        this.cnct = cnct;
    }

    public UserInput getUserInput() throws SQLException{
        UserInput ui = null;
        PreparedStatement ppstmt1 = null, ppstmt2 = null;

        try{
            ppstmt1 = cnct.prepareStatement("SELECT * FROM " + Constants.TABLE_TASK_INFO + " ORDER BY ID LIMIT 1");
            ppstmt2 = cnct.prepareStatement("DELETE FROM " + Constants.TABLE_TASK_INFO + " ORDER BY ID LIMIT 1");
            ResultSet rs = ppstmt1.executeQuery();

            if (rs.next()){
                Gson gson = new Gson();
                ui = gson.fromJson(rs.getString("JSON"), UserInput.class);
            }
            ppstmt2.executeUpdate();
        }
        catch (SQLException sqlerr){
            throw sqlerr;
        }
        finally {
            if (ppstmt1 != null){
                ppstmt1.close();
            }
            if (ppstmt2 != null){
                ppstmt1.close();
            }
        }

        return ui;
    }
}
