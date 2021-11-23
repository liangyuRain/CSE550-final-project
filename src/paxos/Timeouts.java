package paxos;

import application.AMOCommand;
import lombok.Data;

@Data
final class ClientTimeout implements Timeout {
    private static final int CLIENT_RETRY_MILLIS = 1000;

    private final AMOCommand command;

    @Override
    public int timeoutLengthMillis() {
        return CLIENT_RETRY_MILLIS;
    }
}

@Data
final class PingTimeout implements Timeout {
    private static final int PING_INTERVAL_MILLIS = 1000;

    @Override
    public int timeoutLengthMillis() {
        return PING_INTERVAL_MILLIS;
    }
}

@Data
final class PrepareRequestTimeout implements Timeout {
    private static final int PREPARE_TIMEOUT = 1000;

    @Override
    public int timeoutLengthMillis() {
        return PREPARE_TIMEOUT;
    }
}

@Data
final class AcceptRequestTimeout implements Timeout {
    private static final int ACCEPT_TIMEOUT = 1000;

    @Override
    public int timeoutLengthMillis() {
        return ACCEPT_TIMEOUT;
    }
}
