package dao;

import java.sql.Connection;

/**
 * Created by sky on 2017/3/15.
 */
public abstract class Repository {
    protected Connection dbConnection = null;

    public Repository(Connection dbConnection) {
        this.dbConnection = dbConnection;
    }


    protected void assertConnectionValid(){
        if(this.dbConnection ==null){
            throw new RuntimeException("fuck, database is not connected");
        }
    }
}

