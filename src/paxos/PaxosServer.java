package paxos;

import application.*;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {

    private static final int MESSAGE_HANDLER_EXECUTOR_NUM_OF_THREAD = 20;
    private static final int MESSAGE_HANDLER_EXECUTOR_QUEUE_SIZE = 100;

    private static final int MAX_NUM_OF_COMMAND_PER_MESSAGE = 10000;
    private static final int MAX_NUM_OF_COMMAND_IN_MEMORY = 1048576; // 2^20

    private final Address[] servers;
    private final int selfIndex;

    private Address leader; // current leader address
    private final Map<Address, Boolean> alive; // alive server same as lab2
    private final AMOApplication app; // AMO application wrapper
    private final Map<Address, Integer> serverExecuted; // the latest executed command slot num for each server (gc purpose)
    private int maxServerExecuted;

    private final Slots executed; // executed command slots
    private LinkedHashSet<AMOCommand> uncertain; // uncertain command slots.
    // For acceptors, they are newly accepted commands, but possibly
    // rejected because of only minority acceptors accept.
    // For leader, they are commands waiting to be accepted.
    // LinkedHashSet can preserve order of iteration
    private Leader leaderRole; // constructed when acting as leader
    private final Acceptor acceptorRole; // constructed all the time

    boolean prepareTimeoutSet, acceptTimeoutSet; // specific timeout is set

    // used to store acceptor role info when promoted as leader
    Triple<Integer, Address, Integer> prevAcceptorAcceptedNum;
    Pair<Slots, LinkedHashSet<AMOCommand>> prevAcceptorState;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app)
            throws IOException, NoSuchFieldException, IllegalAccessException {
        super(address,
                new ThreadPoolExecutor(MESSAGE_HANDLER_EXECUTOR_NUM_OF_THREAD,
                        MESSAGE_HANDLER_EXECUTOR_NUM_OF_THREAD, 0, TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueueSet<>(MESSAGE_HANDLER_EXECUTOR_QUEUE_SIZE),
                        new ThreadPoolExecutor.DiscardPolicy()),
                10);
        this.servers = servers;
        Arrays.sort(this.servers); // convenient for choosing highest address leader
        this.selfIndex = Arrays.binarySearch(this.servers, address());
        if (this.selfIndex == -1) {
            throw new IllegalStateException("Servers array must contain itself");
        }

        this.alive = new HashMap<>();
        this.app = new AMOApplication(app);
        this.serverExecuted = new HashMap<>();
        this.maxServerExecuted = 0;
        this.executed = new Slots(new ArrayDeque<>(), 0, false);
        this.acceptorRole = new Acceptor();
    }

    @Override
    public void init() {
        super.init();
        Address thisAddr = address();
        for (Address addr : servers) {
            if (!addr.equals(thisAddr)) {
                serverExecuted.put(addr, 0);
            }
        }
        this.leader = address(); // every server is initialized with themselves as leader
        this.uncertain = new LinkedHashSet<>();
        set(new PingTimeout());
    }

    private void received(Address sender) {
        if (alive.put(sender, true) == null) {
            log(Level.INFO, String.format("Server %s revived", sender.hostname()));
        }
        if (sender.compareTo(leader) > 0 && isUpToDate(sender)) {
            // new leader found
            leader = sender;
            log(Level.INFO, String.format("Server %s promoted as leader", leader.hostname()));
            if (!leader.equals(address())) {
                leaderRole = null;
                if (acceptorRole.maxAcceptNum.equals(prevAcceptorAcceptedNum)) { // restore acceptor state after demotion
                    uncertain = prevAcceptorState.getRight();
                    prevAcceptorAcceptedNum = null;
                    prevAcceptorState = null;
                }
            }
        }
    }

    private boolean isUpToDate(Address sender) {
        if (sender.equals(address())) {
            return executed.end() >= maxServerExecuted - MAX_NUM_OF_COMMAND_PER_MESSAGE;
        } else {
            return serverExecuted.get(sender) >= maxServerExecuted - MAX_NUM_OF_COMMAND_PER_MESSAGE;
        }
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePing(Ping m, Address sender) {
        if (alive.put(sender, true) == null) {
            log(Level.INFO, String.format("Server %s revived", sender.hostname()));
        }
        int minNum = Integer.MAX_VALUE;
        for (Map.Entry<Address, Integer> entry : m.serverExecuted()) {
            Address server = entry.getKey();
            int serverEnd = entry.getValue();

            if (server.equals(address())) continue;
            if (serverEnd < minNum) minNum = serverEnd;

            int oldNum = serverExecuted.get(server);
            if (serverEnd > oldNum) {
                serverExecuted.put(server, serverEnd);
                if (serverEnd > maxServerExecuted) {
                    maxServerExecuted = serverEnd;
                }
            }
        }
        executed.gc(minNum);
        electLeader();
    }

    private synchronized void handleRecover(Recover m, Address sender) {
        garbageCollect(m.nextToExecute(), sender);
        received(sender);
        execute(m.executed().commands, m.executed().begin);
        if (address().compareTo(leader) > 0 && isUpToDate(address())) {
            leader = address();
            log(Level.INFO, String.format("Server %s promoted as leader", leader.hostname()));
        }
    }

    @Override
    protected boolean messageFilter(Message message, Address sender) {
        if (executed.commands.size() > MAX_NUM_OF_COMMAND_IN_MEMORY - MAX_NUM_OF_COMMAND_PER_MESSAGE) {
            return message instanceof Ping;
        }
        if (message instanceof PaxosRequest) {
            return leader.equals(address()) &&
                    messageHandlerExecutor.getQueue().remainingCapacity() > MESSAGE_HANDLER_EXECUTOR_QUEUE_SIZE / 2;
        }
        return true;
    }

    private synchronized void handlePaxosRequest(PaxosRequest m, Address sender) {
        if (!leader.equals(address())) return;
        if (leaderRole == null) { // lazy construction
            leaderRole = new Leader();
        }
        if (app.alreadyExecuted(m.command())) { // direct reply is executed
            AMOResult result = app.execute(m.command());
            if (result != null) {
                send(new PaxosReply(leader, result), result.clientAddr());
            }
        } else if (!uncertain.contains(m.command()) && leaderRole.pendingRequests.add(m.command())) {
            if (leaderRole.noPrepareReply == null) { // sending prepare requests
                leaderRole.sendPrepare();
            } else if (leaderRole.noAcceptReply == null &&
                    leaderRole.noPrepareReply.size() < servers.length / 2.0) { // skip prepare and directly into accept
                leaderRole.sendAccept();
            } // else: the system is still at accepting states, all commands go to pendingRequests
        }
    }

    private synchronized void handlePrepareReply(PrepareReply m, Address sender) {
        if (m.executed() != null) {
            garbageCollect(m.executed().end(), sender);
        }
        received(sender);
        if (!leader.equals(address())) return;
        if (leaderRole == null) {
            leaderRole = new Leader();
        }
        if (leaderRole.proposalNum.equals(m.proposalNum())) {
            if (leaderRole.noPrepareReply != null) {
                if (leaderRole.noPrepareReply.remove(sender) && leaderRole.noAcceptReply == null) {
                    if (m.maxProposalNum().compareTo(m.proposalNum()) <= 0) {
                        if (leaderRole.maxAcceptedNum == null ||
                                leaderRole.maxAcceptedNum.compareTo(m.maxAcceptNum()) < 0) {
                            leaderRole.maxAcceptedNum = m.maxAcceptNum();
                            leaderRole.maxState = Pair.of(m.executed(), m.uncertain());
                        }
                        if (leaderRole.noPrepareReply.size() < servers.length / 2.0) { // majority has replied
                            if (prevAcceptorAcceptedNum != null) {
                                if (prevAcceptorAcceptedNum.compareTo(leaderRole.maxAcceptedNum) > 0) {
                                    // compare with self acceptor state
                                    leaderRole.maxAcceptedNum = prevAcceptorAcceptedNum;
                                    leaderRole.maxState = prevAcceptorState;
                                }
                                prevAcceptorAcceptedNum = null;
                                prevAcceptorState = null;
                            }
                            // sync with max accepted number acceptor's state, including the uncertain one.
                            // It is guaranteed in the protocol that the acceptor with highest accepted number is
                            // majority.
                            if (leaderRole.maxAcceptedNum.getMiddle().equals(address())) {
                                execute(leaderRole.maxState.getLeft().commands, leaderRole.maxState.getLeft().begin);
                                execute(leaderRole.maxState.getRight(), leaderRole.maxState.getLeft().end());
                            } else {
                                // the leader issue the max accept number has crashed, and its accept request may only
                                // be accepted by minority. Therefore, cannot directly execute the state.
                                execute(leaderRole.maxState.getLeft().commands, leaderRole.maxState.getLeft().begin);
                                addUncertain(leaderRole.maxState.getRight(), leaderRole.maxState.getLeft().end());
                            }
                            if (!leaderRole.pendingRequests.isEmpty()) {
                                leaderRole.sendAccept();
                            }
                        }
                    } else { // prepare request rejected, adjust prepare num and resend
                        leaderRole.newProposal(m.maxProposalNum());
                        leaderRole.sendPrepare();
                    }
                }
            }
        }
    }

    private synchronized void handleAcceptReply(AcceptReply m, Address sender) {
        garbageCollect(m.nextToExecute(), sender);
        received(sender);
        if (!leader.equals(address())) return;
        if (leaderRole == null) {
            leaderRole = new Leader();
        }
        Triple<Integer, Address, Integer> current =
                Triple.of(leaderRole.proposalNum.left, leaderRole.proposalNum.right, leaderRole.acceptRound);
        if (current.equals(m.acceptNum())) {
            if (leaderRole.noAcceptReply != null) {
                if (leaderRole.noAcceptReply.remove(sender)) {
                    if (m.maxProposalNum().compareTo(leaderRole.proposalNum) <= 0) {
                        if (leaderRole.noAcceptReply.size() < servers.length / 2.0) { // majority replied
                            for (AMOCommand c : uncertain) { // execute all commands in uncertain since majority replied
                                if (app.alreadyExecuted(c)) {
                                    throw new IllegalStateException(String.format(
                                            "Slot %d command already executed: %s", this.executed.end(), c));
                                }
                                AMOResult result = app.execute(c);
                                log(Level.FINE, String.format("Executed slot %d with command %s, result: %s",
                                        this.executed.end(), c, result));
                                executed.commands.add(c);
                                send(new PaxosReply(leader, result), result.clientAddr()); // reply clients
                            }
                            uncertain.clear();
                            leaderRole.newAccept();
                            if (!leaderRole.pendingRequests.isEmpty()) {
                                leaderRole.sendAccept();
                            }
                        }
                    } else { // accept request rejected, back to prepare phase
                        leaderRole.newProposal(m.maxProposalNum());
                        leaderRole.sendPrepare();
                    }
                }
            }
        }
    }

    private synchronized void handlePrepareRequest(PrepareRequest m, Address sender) {
        garbageCollect(m.nextToExecute(), sender);
        received(sender);
        if (!leader.equals(sender)) return;
        if (!isUpToDate(sender)) return;
        if (m.proposalNum().compareTo(acceptorRole.maxPrepareNum) >= 0) { // respond with promise
            acceptorRole.maxPrepareNum = m.proposalNum();
            send(new PrepareReply(m.proposalNum(), executed.startFrom(serverExecuted.get(sender)), uncertain,
                    acceptorRole.maxAcceptNum, acceptorRole.maxPrepareNum, false), sender);
        } else { // reject
            send(new PrepareReply(m.proposalNum(), null, null,
                    acceptorRole.maxAcceptNum, acceptorRole.maxPrepareNum, false), sender);
        }
    }

    private synchronized void handleAcceptRequest(AcceptRequest m, Address sender) {
        garbageCollect(m.executed().end(), sender);
        received(sender);
        if (!leader.equals(sender)) return;
        if (m.acceptNum().compareTo(acceptorRole.maxAcceptNum) >= 0) { // accept the request and sync with leader state
            acceptorRole.maxPrepareNum = ImmutablePair.of(m.acceptNum().getLeft(), m.acceptNum().getMiddle());
            acceptorRole.maxAcceptNum = m.acceptNum();
            execute(m.executed().commands, m.executed().begin);
            uncertain = m.uncertain().stream()
                    .filter(((Predicate<AMOCommand>) app::alreadyExecuted).negate())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            // the proposed commands may be rejected by other majority of servers, so they cannot be executed and stored
            // in uncertain
        }
        send(new AcceptReply(m.acceptNum(), acceptorRole.maxPrepareNum, executed.end()), sender);
    }

    /* -------------------------------------------------------------------------
        Timeout Handlers
       -----------------------------------------------------------------------*/
    private boolean check = false;

    private synchronized void onPingTimeout(PingTimeout t) {
        if (check) {
            Iterator<Map.Entry<Address, Boolean>> iter = alive.entrySet().iterator();
            while (iter.hasNext()) { // check all servers' aliveness, optimized using iterator
                Map.Entry<Address, Boolean> entry = iter.next();
                if (!entry.getValue()) {
                    log(Level.INFO, String.format("Server %s dead", entry.getKey().hostname()));
                    iter.remove();
                } else {
                    entry.setValue(false);
                }
            }
        }
        check = !check;
        alive.put(address(), true);
        electLeader();
        if (!leader.equals(address()) ||
                serverExecuted.keySet().stream().filter(alive::containsKey).noneMatch(this::isUpToDate)) {
            for (int i = (selfIndex + 1) % servers.length; i != selfIndex; i = (i + 1) % servers.length) {
                if (alive.containsKey(servers[i]) && !leader.equals(servers[i])) {
                    int serverEnd = serverExecuted.get(servers[i]);
                    if (serverEnd < executed.end() - MAX_NUM_OF_COMMAND_PER_MESSAGE / 10) {
                        send(new Recover(executed.startFrom(
                                serverEnd, MAX_NUM_OF_COMMAND_PER_MESSAGE), executed.end(), false), servers[i]);
                    }
                    break;
                }
            }
        }
        List<ImmutablePair<Address, Integer>> l = serverExecuted.entrySet().stream()
                .map(ImmutablePair::of)
                .collect(Collectors.toList());
        l.add(ImmutablePair.of(address(), executed.end()));
        broadcast(new Ping(l, true), serverExecuted.keySet()); // broadcast ping message
        set(t);
    }

    private synchronized void onPrepareRequestTimeout(PrepareRequestTimeout t) {
        if (leader.equals(address()) && leaderRole != null && leaderRole.noPrepareReply != null &&
                leaderRole.noPrepareReply.size() >= servers.length / 2.0) {
            set(t);
            List<Address> aliveDests = leaderRole.noPrepareReply.stream()
                    .filter(alive::containsKey)
                    .collect(Collectors.toList());
            broadcast(new PrepareRequest(ImmutablePair.of(leaderRole.proposalNum), executed.end()), aliveDests);
        } else {
            prepareTimeoutSet = false;
        }
    }

    private synchronized void onAcceptRequestTimeout(AcceptRequestTimeout t) {
        if (leader.equals(address()) && leaderRole != null && leaderRole.noPrepareReply != null &&
                leaderRole.noPrepareReply.size() < servers.length / 2.0 &&
                leaderRole.noAcceptReply != null &&
                leaderRole.noAcceptReply.size() >= servers.length / 2.0) {
            set(t);
            List<Address> senders = leaderRole.noPrepareReply.stream()
                    .filter(alive::containsKey)
                    .filter(leaderRole.noAcceptReply::contains)
                    .collect(Collectors.toList());
            if (!senders.isEmpty()) { // in case some server still not receive prepare request so cannot respond
                // accept request. Broadcast prepare request to all servers without neither
                // prepare reply nor accept reply
                broadcast(new PrepareRequest(ImmutablePair.of(leaderRole.proposalNum), executed.end()), senders);
            }
            leaderRole.noAcceptReply.stream()
                    .filter(alive::containsKey)
                    .filter(this::isUpToDate)
                    .forEach(addr -> send(new AcceptRequest(
                            ImmutableTriple.of(
                                    leaderRole.proposalNum.left, leaderRole.proposalNum.right, leaderRole.acceptRound),
                            executed.startFrom(serverExecuted.get(addr)), uncertain, false), addr));
        } else {
            acceptTimeoutSet = false;
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/
    @ToString
    @EqualsAndHashCode
    private class Leader implements Serializable {

        HashSet<AMOCommand> pendingRequests; // requests that received but not in any phase of paxos
        MutablePair<Integer, Address> proposalNum; // current proposal number
        int acceptRound; // the round of accept request. if prepare is succeeded, multiple rounds of accept is proceeded
        // without prepare

        Set<Address> noPrepareReply, noAcceptReply; // servers without prepare reply or accept reply
        Triple<Integer, Address, Integer> maxAcceptedNum; // max accepted num acceptor in prepare phase
        Pair<Slots, LinkedHashSet<AMOCommand>> maxState; // state of the max accepted num acceptor

        PrepareRequestTimeout prepareTimeout;
        AcceptRequestTimeout acceptTimeout;

        Leader() {
            this.pendingRequests = new HashSet<>();
            this.proposalNum = new MutablePair<>(acceptorRole.maxPrepareNum.getLeft() + 1, address());
            this.acceptRound = 0;

            this.noPrepareReply = noAcceptReply = null;
            prevAcceptorAcceptedNum = acceptorRole.maxAcceptNum;
            prevAcceptorState = Pair.of(executed, new LinkedHashSet<>(uncertain));
            uncertain.clear();

            this.maxAcceptedNum = null;
            this.maxState = null;

            this.prepareTimeout = new PrepareRequestTimeout();
            this.acceptTimeout = new AcceptRequestTimeout();
        }

        void newProposal(Pair<Integer, Address> num) {
            pendingRequests.addAll(uncertain); // all uncertain commands enter pendingRequest to start all over
            uncertain.clear();
            ++this.proposalNum.left;
            acceptRound = 0;
            if (num != null && num.compareTo(this.proposalNum) >= 0) {
                this.proposalNum.left = num.getLeft() + 1;
            }
            this.noPrepareReply = this.noAcceptReply = null;
            this.maxAcceptedNum = null;
            this.maxState = null;
        }

        void newAccept() { // new accept round without prepare
            ++acceptRound;
            this.noAcceptReply = null;
            this.maxAcceptedNum = null;
            this.maxState = null;
        }

        void sendPrepare() {
            this.noPrepareReply = new HashSet<>(serverExecuted.keySet());
            if (!prepareTimeoutSet) {
                prepareTimeoutSet = true;
                set(prepareTimeout);
            }
            acceptorRole.maxPrepareNum = ImmutablePair.of(proposalNum);
            broadcast(new PrepareRequest(ImmutablePair.of(proposalNum), executed.end()), noPrepareReply);
        }

        void sendAccept() {
            uncertain.addAll(pendingRequests); // all pending requests enter uncertain and fixed
            pendingRequests.clear();
            this.noAcceptReply = new HashSet<>(serverExecuted.keySet());
            if (!acceptTimeoutSet) {
                acceptTimeoutSet = true;
                set(acceptTimeout);
            }
            acceptorRole.maxAcceptNum = ImmutableTriple.of(proposalNum.left, proposalNum.right, acceptRound);
            noAcceptReply.stream()
                    .filter(alive::containsKey)
                    .filter(PaxosServer.this::isUpToDate)
                    .forEach(addr -> send(new AcceptRequest(
                            ImmutableTriple.of(proposalNum.left, proposalNum.right, acceptRound),
                            executed.startFrom(serverExecuted.get(addr)), uncertain, false), addr));
        }

    }

    @ToString
    @EqualsAndHashCode
    private class Acceptor implements Serializable {

        ImmutablePair<Integer, Address> maxPrepareNum; // maximum prepare num
        ImmutableTriple<Integer, Address, Integer> maxAcceptNum; // maximum accept num (prepare num + accept round)

        Acceptor() {
            maxPrepareNum = ImmutablePair.of(-1, address());
            maxAcceptNum = ImmutableTriple.of(-1, address(), 0);
        }

    }

    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor
    static class Slots implements Serializable, Copyable { // data structure to represents slots

        ArrayDeque<AMOCommand> commands;
        int begin; // the slot number of the first command

        @EqualsAndHashCode.Exclude
        final boolean copied;

        @ToString.Include
        int end() {
            return begin + commands.size();
        }

        void gc(int min) {
            if (end() < min) {
                min = end();
            }
            while (begin < min) {
                commands.remove();
                ++begin;
            }
        }

        Slots startFrom(int begin) {
            return startFrom(begin, Integer.MAX_VALUE);
        }

        Slots startFrom(int begin, int maxSize) {
            if (begin <= this.begin && commands.size() <= maxSize) {
                return this;
            }

            int end = end();
            if (begin > end) {
                begin = end;
            }

            // Assume AMOCommand itself is immutable
            return new Slots(
                    this.commands.stream()
                            .skip(begin - this.begin)
                            .limit(maxSize)
                            .collect(Collectors.toCollection(ArrayDeque::new)),
                    begin, true);
        }

        @Override
        public Slots immutableCopy() {
            if (this.copied) {
                return this;
            } else {
                // Assume AMOCommand itself is immutable
                return new Slots(new ArrayDeque<>(commands), this.begin, true);
            }
        }

    }

    private void garbageCollect(int nextToExecute, Address sender) {
        int oldNum = serverExecuted.get(sender);
        if (nextToExecute > oldNum) {
            serverExecuted.put(sender, nextToExecute);
            if (nextToExecute > maxServerExecuted) {
                maxServerExecuted = nextToExecute;
            }
            if (nextToExecute > executed.begin) {
                // garbage collect commands that every server has executed
                executed.gc(serverExecuted.values().stream().min(Integer::compare).get());
            }
        }
    }

    private void electLeader() {
        for (int i = servers.length - 1; i >= 0; --i) {
            if (alive.containsKey(servers[i]) && isUpToDate(servers[i])) {
                if (!leader.equals(servers[i])) {
                    log(Level.INFO, String.format("Server %s promoted as leader", leader.hostname()));
                }
                leader = servers[i];
                if (!leader.equals(address())) {
                    leaderRole = null;
                    if (acceptorRole.maxAcceptNum.equals(prevAcceptorAcceptedNum)) { // restore acceptor state after demotion
                        uncertain = prevAcceptorState.getRight();
                        prevAcceptorAcceptedNum = null;
                        prevAcceptorState = null;
                    }
                }
                break;
            }
        }
    }

    // execute and record other sequence of commands without repeat using slot number
    private void execute(Collection<AMOCommand> commands, int begin) {
        if (begin > this.executed.end()) {
            throw new IllegalStateException("Missing command");
        }
        if (this.executed.end() < begin + commands.size()) {
            commands.stream()
                    .skip(Math.max(executed.end() - begin, 0))
                    .forEach(command -> {
                        if (app.alreadyExecuted(command)) {
                            throw new IllegalStateException(String.format(
                                    "Slot %d command already executed: %s", this.executed.end(), command));
                        }
                        AMOResult result = app.execute(command);
                        log(Level.FINE, String.format("Executed slot %d with command %s, result: %s",
                                this.executed.end(), command, result));
                        executed.commands.add(command);
                        uncertain.remove(command);
                        if (leaderRole != null) {
                            leaderRole.pendingRequests.remove(command);
                        }
                    });
        }
    }

    private void addUncertain(Collection<AMOCommand> commands, int begin) {
        int end = executed.end() + uncertain.size();
        if (end < begin + commands.size()) {
            commands.stream()
                    .skip(Math.max(end - begin, 0))
                    .filter(((Predicate<AMOCommand>) app::alreadyExecuted).negate())
                    .forEach(uncertain::add);
        }
    }

    /* -------------------------------------------------------------------------
        Main Method
       -----------------------------------------------------------------------*/

    public static void main(String[] args) throws IOException, NoSuchFieldException, IllegalAccessException {
        if (args.length < 2) {
            System.out.println("Usage: java -jar paxos_server.jar [local IPv4 address] [server ips config]");
            System.exit(1);
        }

        Address localAddr = Address.parseIPv4(args[0]);
        Address[] addrs = Address.getServerAddresses(args[1]);
        PaxosServer server = new PaxosServer(localAddr, addrs, new LockApplication());
        server.setLogLevel(Level.FINER);
        server.init();
        server.listen();
    }

    /* -------------------------------------------------------------------------
        Log
       -----------------------------------------------------------------------*/

    private int lastExecuteEnd = 0;
    private long lastTimeStamp = System.nanoTime();

    @Override
    protected void logEssential(StringBuilder sb) {
        super.logEssential(sb);
        sb.append(System.lineSeparator());

        int executeEnd;
        int numOfSlotsInMemory;
        HashMap<Address, Integer> serverExecutedCopy;
        synchronized (this) {
            executeEnd = executed.end();
            numOfSlotsInMemory = executed.commands.size();
            serverExecutedCopy = new HashMap<>(serverExecuted);
        }

        long timestamp = System.nanoTime();
        double throughput = (executeEnd - lastExecuteEnd) * 1.0e9 / (timestamp - lastTimeStamp);
        double avgTimeCost = (timestamp - lastTimeStamp) / 1.0e6 / (executeEnd - lastExecuteEnd);
        sb.append(
                String.format("Paxos info: Executed: %d, Throughput: %.3f, " +
                                "Avg time cost: %.3f ms, Num of commands in memory: %d%n" +
                                "Server Alive: [%s]%n" +
                                "Server Executed: %n{%s}%n" +
                                "Leader: %s",
                        executeEnd, throughput, avgTimeCost, numOfSlotsInMemory,
                        alive.keySet().stream()
                                .sorted()
                                .map(Address::hostname)
                                .reduce((s1, s2) -> String.join(", ", s1, s2))
                                .orElse(""),
                        serverExecutedCopy.entrySet().stream()
                                .sorted(Map.Entry.comparingByKey())
                                .map(entry -> String.format(
                                        "(%s): %s", entry.getKey().hostname(), entry.getValue()))
                                .reduce((s1, s2) -> String.join(", ", s1, s2))
                                .get(),
                        leader.hostname())
        );

        if (executed.commands.size() > MAX_NUM_OF_COMMAND_IN_MEMORY - MAX_NUM_OF_COMMAND_PER_MESSAGE) {
            sb.append(System.lineSeparator());
            sb.append("Paxos memory limit reached");
        }

        lastExecuteEnd = executeEnd;
        lastTimeStamp = timestamp;
    }

}
