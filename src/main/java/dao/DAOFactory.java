package dao;

import java.sql.Connection;

/**
 * Created by sky on 2017/3/15.
 */
public class DAOFactory {
    static public TaskRecordDAO getTRDAO(Connection cnct){
        return new TaskRecordDAO(cnct);
    }

    static public UserInputDAO getUIDAO(Connection cnct){
        return new UserInputDAO(cnct);
    }
}
