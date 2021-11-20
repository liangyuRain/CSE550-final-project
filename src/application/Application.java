package application;

import java.io.Serializable;

public interface Application extends Serializable {
    Result execute(Command command);
}
