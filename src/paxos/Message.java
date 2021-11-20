package paxos;

import lombok.Data;

import java.io.Serializable;

public interface Message extends Serializable {

}

@Data
class Package {

    private final Address sender;
    private final Message message;

}
