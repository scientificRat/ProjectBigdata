package constants;

/**
 * Created by sky on 2017/3/11.
 */
public interface Constants {
    String SPARK_APP_NAME = "Statistic";

    String LOCAL_SESSION_DATA_PATH = "localData/click.log";
    String LOCAL_USER_DATA_PATH = "localData/user.txt";
    String LOCAL_PRODUCT_DATA_PATH = "localData/product.txt";


    String TABLE_USER_INFO = "user_info";
    String TABLE_PRODUCT_INFO = "product_info";
    String TABLE_USER_VISIT_ACTION = "user_visit_action";

    boolean USING_RDD = false;
}
