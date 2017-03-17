package constants;

/**
 * Created by sky on 2017/3/11.
 */
public interface Constants {
    String SPARK_APP_NAME = "Statistic";

    String LOCAL_SESSION_DATA_PATH = "localData/click_shrink.log";
    String LOCAL_USER_DATA_PATH = "localData/user.txt";
    String LOCAL_PRODUCT_DATA_PATH = "localData/product.txt";

    String TABLE_TASK_INFO = "idcproj";
    String TABLE_TASK_RECORD_INFO = "taskrecord";
    String TABLE_USER_INFO = "userdata";
    String TABLE_PRODUCT_INFO = "productdata";
    String TABLE_USER_VISIT_ACTION = "sessionrecord";

    boolean USING_RDD = false;
}
