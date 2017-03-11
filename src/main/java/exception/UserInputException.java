package exception;

import java.io.IOException;

/**
 * Created by sky on 2017/3/11.
 */
public class UserInputException extends IOException {
    public UserInputException(String message) {
        super(message);
    }
}
