package application;

import java.io.Serializable;

public interface Command extends Serializable {

    default boolean readOnly() {
        return false;
    }

}
