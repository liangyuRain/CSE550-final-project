package paxos;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

public interface Message extends Serializable, Logged, Copyable {
    @Override
    Message immutableCopy();
}

@ToString
@EqualsAndHashCode
class Package implements Serializable {

    @Getter
    private final Address sender;
    @Getter
    private final int hash;
    @Getter
    private final Message message;

    public Package(Address sender, Message message) {
        this.sender = sender;
        this.message = message;
        this.hash = message.hashCode();
    }

}
