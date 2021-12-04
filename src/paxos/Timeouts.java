package paxos;

import application.AMOCommand;
import lombok.Data;

import java.util.logging.Level;

@Data
final class ClientTimeout implements Timeout {
    private static final int CLIENT_RETRY_MILLIS = 1000;

    private final AMOCommand command;

    @Override
    public int timeoutLengthMillis() {
        return CLIENT_RETRY_MILLIS;
    }

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }
}

@Data
final class PingTimeout implements Timeout {
    private static final int PING_INTERVAL_MILLIS = 50;

    @Override
    public int timeoutLengthMillis() {
        return PING_INTERVAL_MILLIS;
    }

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }
}

@Data
final class PrepareRequestTimeout implements Timeout {
    private static final int PREPARE_TIMEOUT = 50;

    @Override
    public int timeoutLengthMillis() {
        return PREPARE_TIMEOUT;
    }

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }
}

@Data
final class AcceptRequestTimeout implements Timeout {
    private static final int ACCEPT_TIMEOUT = 50;

    @Override
    public int timeoutLengthMillis() {
        return ACCEPT_TIMEOUT;
    }

    @Override
    public Level logLevel() {
        return Level.FINEST;
    }
}
