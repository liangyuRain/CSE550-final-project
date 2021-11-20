package paxos;

import application.Command;
import application.Result;

public interface Client {

    void sendCommand(Command command);

    boolean hasResult();

    Result getResult() throws InterruptedException;

}
