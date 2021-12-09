package application;

import paxos.Address;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication implements Application {
    @Getter
    @NonNull
    private final Application application;

    private final Map<Address, AMOResult> executed = new HashMap<>();

    @Override
    public synchronized AMOResult execute(Command command) {
        assert command != null;

        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        // Your code here...
        AMOResult result;
        if (alreadyExecuted(amoCommand)) {
            result = executed.get(amoCommand.clientAddr());
            if (result.sequenceNum() != amoCommand.sequenceNum()) {
                result = null;
            }
        } else {
            result = new AMOResult(application.execute(amoCommand.command()),
                    amoCommand.sequenceNum(), amoCommand.clientAddr());
            executed.put(amoCommand.clientAddr(), result);
        }
        return result;
    }

    public synchronized Result executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }

        if (command instanceof AMOCommand) {
            return execute(command);
        }

        return application.execute(command);
    }

    public synchronized boolean alreadyExecuted(AMOCommand amoCommand) {
        AMOResult result = executed.get(amoCommand.clientAddr());
        return result != null && amoCommand.sequenceNum() <= result.sequenceNum();
    }
}
