package paxos;

import lombok.Data;

import java.io.Serializable;

public interface Message extends Serializable, Logged {
}

@Data
class Package implements Serializable {

    private final Address sender;
    private final Message message;

}
