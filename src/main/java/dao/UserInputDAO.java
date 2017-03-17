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
        PreparedStatement ppstmt = null;

        try{
            ppstmt = cnct.prepareStatement("SELECT * FROM " + Constants.TABLE_TASK_INFO + " ORDER BY ID LIMIT 1");
            ResultSet rs = ppstmt.executeQuery();

            if (rs.next()){
                Gson gson = new Gson();
                System.out.println(rs.getString("JSON"));
                ui = gson.fromJson(rs.getString("JSON"), UserInput.class);
            }
        }
        catch (SQLException sqlerr){
            throw sqlerr;
        }
        finally {
            if (ppstmt != null){
                ppstmt.close();
            }
        }

        return ui;
    }
}
