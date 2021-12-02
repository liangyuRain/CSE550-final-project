package paxos;

import application.*;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    private final Address[] servers;

    private Address leader; // current leader address
    private Map<Address, Boolean> alive; // alive server same as lab2
    private AMOApplication app; // AMO application wrapper
    private Map<Address, Integer> serverExecuted; // the latest executed command slot num for each server (gc purpose)

    private Slots executed; // executed command slots
    private LinkedHashSet<AMOCommand> uncertain; // uncertain command slots.
    // For acceptors, they are newly accepted commands, but possibly
    // rejected because of only minority acceptors accept.
    // For leader, they are commands waiting to be accepted.
    // LinkedHashSet can preserve order of iteration
    private Leader leaderRole; // constructed when acting as leader
    private Acceptor acceptorRole; // constructed all the time

    boolean prepareTimeoutSet, acceptTimeoutSet; // specific timeout is set

    // used to store acceptor role info when promoted as leader
    Triple<Integer, Address, Integer> prevAcceptorAcceptedNum;
    Pair<Slots, LinkedHashSet<AMOCommand>> prevAcceptorState;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) throws IOException {
        super(address);
        this.servers = servers;

        this.app = new AMOApplication(app);
    }

    @Override
    public void init() {
        super.init();
        Address thisAddr = address();
        Arrays.sort(servers); // convenient for choosing highest address leader
        this.serverExecuted = new HashMap<>();
        for (Address addr : servers) {
            if (!addr.equals(thisAddr)) {
                serverExecuted.put(addr, 0);
            }
        }
        this.leader = address(); // every server is initialized with themselves as leader
        this.alive = new HashMap<>();
        this.acceptorRole = new Acceptor();
        this.executed = new Slots();
        this.uncertain = new LinkedHashSet<>();
        set(new PingTimeout());
    }

    private void received(Address sender) {
        if (sender.compareTo(leader) > 0) { // new leader found
            leader = sender;
            super.log(Level.INFO, String.format("Server %s promoted as leader", leader.hostname()));
        }
        if (!leader.equals(address())) {
            leaderRole = null;
            if (acceptorRole.maxAcceptNum.equals(prevAcceptorAcceptedNum)) { // restore acceptor state after demotion
                uncertain = prevAcceptorState.getRight();
                prevAcceptorAcceptedNum = null;
                prevAcceptorState = null;
            }
        }
        if (!alive.containsKey(sender)) {
            super.log(Level.INFO, String.format("Server %s revived", sender.hostname()));
        }
        alive.put(sender, true);
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePing(Ping m, Address sender) {
        received(sender);
        garbageCollect(m.nextToExecute(), sender);
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
                                if (execute(leaderRole.maxState.getLeft().commands, leaderRole.maxState.getLeft().begin)) {
                                    leaderRole.pendingRequests.removeAll(leaderRole.maxState.getLeft().commands);
                                }
                                if (execute(leaderRole.maxState.getRight(), leaderRole.maxState.getLeft().end())) {
                                    leaderRole.pendingRequests.removeAll(leaderRole.maxState.getRight());
                                }
                            } else {
                                // the leader issue the max accept number has crashed, and its accept request may only
                                // be accepted by minority. Therefore, cannot directly execute the state.
                                addUncertain(leaderRole.maxState.getLeft().commands, leaderRole.maxState.getLeft().begin);
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
        received(sender);
        garbageCollect(m.nextToExecute(), sender);
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
                                AMOResult result = app.execute(c);
                                super.log(Level.FINE, String.format("Executed slot %d with command %s, result: %s",
                                        this.executed.end(), c, result == null ? "null" : result));
                                executed.commands.add(c);
                                if (result != null) { // ancient command
                                    send(new PaxosReply(leader, result), result.clientAddr()); // reply clients
                                }
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
        received(sender);
        if (!leader.equals(sender)) return;
        if (m.proposalNum().compareTo(acceptorRole.maxPrepareNum) >= 0) { // respond with promise
            acceptorRole.maxPrepareNum = m.proposalNum();
            send(new PrepareReply(m.proposalNum(), executed.startFrom(serverExecuted.get(sender)), uncertain,
                    acceptorRole.maxAcceptNum, acceptorRole.maxPrepareNum), sender);
        } else { // reject
            send(new PrepareReply(m.proposalNum(), null, null,
                    acceptorRole.maxAcceptNum, acceptorRole.maxPrepareNum), sender);
        }
    }

    private synchronized void handleAcceptRequest(AcceptRequest m, Address sender) {
        received(sender);
        garbageCollect(m.nextToExecute(), sender);
        if (!leader.equals(sender)) return;
        if (m.acceptNum().compareTo(acceptorRole.maxAcceptNum) >= 0) { // accept the request and sync with leader state
            acceptorRole.maxPrepareNum = Pair.of(m.acceptNum().getLeft(), m.acceptNum().getMiddle());
            acceptorRole.maxAcceptNum = m.acceptNum();
            execute(m.executed().commands, m.executed().begin);
            uncertain = m.uncertain().stream()
                    .filter(((Predicate<AMOCommand>)app::alreadyExecuted).negate())
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
                    super.log(Level.INFO, String.format("Server %s dead", entry.getKey().hostname()));
                    iter.remove();
                } else {
                    entry.setValue(false);
                }
            }
            alive.put(address(), true);
            if (!alive.containsKey(leader)) { // leader no longer alive, choose next highest alive address
                for (int i = servers.length - 1; i >= 0; --i) {
                    if (alive.containsKey(servers[i])) {
                        leader = servers[i];
                        super.log(Level.INFO, String.format("Server %s promoted as leader", leader.hostname()));
                        if (leader.equals(address())) {
                            leaderRole = new Leader();
                        }
                        break;
                    }
                }
            }
        }
        check = !check;
        set(t);
        broadcast(new Ping(executed.end()), serverExecuted.keySet()); // broadcast ping message
    }

    private synchronized void onPrepareRequestTimeout(PrepareRequestTimeout t) {
        if (leader.equals(address()) && !leaderRole.pendingRequests.isEmpty() &&
                leaderRole.noPrepareReply != null &&
                leaderRole.noPrepareReply.size() >= servers.length / 2.0) {
            set(t);
            List<Address> aliveDests = leaderRole.noPrepareReply.stream()
                    .filter(alive::containsKey)
                    .collect(Collectors.toList());
            broadcast(new PrepareRequest(leaderRole.proposalNum), aliveDests);
        } else {
            prepareTimeoutSet = false;
        }
    }

    private synchronized void onAcceptRequestTimeout(AcceptRequestTimeout t) {
        if (leader.equals(address()) && !uncertain.isEmpty() &&
                leaderRole.noPrepareReply != null &&
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
                broadcast(new PrepareRequest(leaderRole.proposalNum), senders);
            }
            List<Address> aliveDests = leaderRole.noAcceptReply.stream()
                    .filter(alive::containsKey)
                    .collect(Collectors.toList());
            broadcast(new AcceptRequest(
                    Triple.of(leaderRole.proposalNum.left, leaderRole.proposalNum.right, leaderRole.acceptRound),
                    executed.startFrom(aliveDests.stream()
                            .map(serverExecuted::get)
                            .min(Integer::compare)
                            .orElse(Integer.MIN_VALUE)),
                    uncertain, executed.end()), aliveDests);
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
            acceptorRole.maxPrepareNum = new ImmutablePair<>(proposalNum.left, proposalNum.right);
            broadcast(new PrepareRequest(proposalNum),
                    noPrepareReply.stream()
                            .filter(alive::containsKey)
                            .collect(Collectors.toList()));
        }

        void sendAccept() {
            uncertain.addAll(pendingRequests); // all pending requests enter uncertain and fixed
            pendingRequests.clear();
            this.noAcceptReply = new HashSet<>(serverExecuted.keySet());
            if (!acceptTimeoutSet) {
                acceptTimeoutSet = true;
                set(acceptTimeout);
            }
            acceptorRole.maxAcceptNum = new ImmutableTriple<>(proposalNum.left, proposalNum.right, acceptRound);
            List<Address> aliveDests = noAcceptReply.stream()
                    .filter(alive::containsKey)
                    .collect(Collectors.toList());
            broadcast(new AcceptRequest(Triple.of(proposalNum.left, proposalNum.right, acceptRound),
                    executed.startFrom(aliveDests.stream()
                            .map(serverExecuted::get)
                            .min(Integer::compare)
                            .orElse(Integer.MIN_VALUE)),
                    uncertain, executed.end()), aliveDests);
        }

    }

    @ToString
    @EqualsAndHashCode
    private class Acceptor implements Serializable {

        Pair<Integer, Address> maxPrepareNum; // maximum prepare num
        Triple<Integer, Address, Integer> maxAcceptNum; // maximum accept num (prepare num + accept round)

        Acceptor() {
            maxPrepareNum = new ImmutablePair<>(-1, address());
            maxAcceptNum = new ImmutableTriple<>(-1, address(), 0);
        }

    }

    @ToString
    @EqualsAndHashCode
    static class Slots implements Serializable { // data structure to represents slots

        LinkedList<AMOCommand> commands;
        int begin; // the slot number of the first command

        Slots() {
            commands = new LinkedList<>();
            begin = 0;
        }

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
            if (begin <= this.begin) {
                return this;
            }

            Slots s = new Slots();
            s.begin = begin;
            s.commands = this.commands.stream()
                    .skip(begin - this.begin)
                    .collect(Collectors.toCollection(LinkedList::new));
            return s;
        }

    }

    private void garbageCollect(int nextToExecute, Address sender) {
        int oldNum = serverExecuted.get(sender);
        if (nextToExecute > oldNum && nextToExecute > executed.begin) {
            serverExecuted.put(sender, nextToExecute);
            executed.gc(serverExecuted.values().stream().min(Integer::compare).get()); // garbage collect commands that
            // every server has executed
        }
    }

    // execute and record other sequence of commands without repeat using slot number
    private boolean execute(Collection<AMOCommand> commands, int begin) {
        if (this.executed.end() < begin + commands.size()) {
            Iterator<AMOCommand> iter = commands.iterator();
            for (int i = begin; i < executed.end(); ++i) {
                iter.next();
            }
            while (iter.hasNext()) {
                AMOCommand command = iter.next();
                AMOResult result = app.execute(command);
                super.log(Level.FINE, String.format("Executed slot %d with command %s, result: %s",
                        this.executed.end(), command, result == null ? "null" : result));
                executed.commands.add(command);
            }
            return true;
        } else {
            return false;
        }
    }

    private void addUncertain(Collection<AMOCommand> commands, int begin) {
        int end = executed.end() + uncertain.size();
        if (end < begin + commands.size()) {
            Iterator<AMOCommand> iter = commands.iterator();
            for (int i = begin; i < end; ++i) {
                iter.next();
            }
            while (iter.hasNext()) {
                uncertain.add(iter.next());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: java -jar paxos_server.jar [server ips config]");
            System.out.println("Missing [server ips config]");
            System.exit(1);
        }

        Address localAddr = Address.getLocalAddress();
        Address[] addrs = Address.getServerAddresses(args[0]);
        PaxosServer server = new PaxosServer(localAddr, addrs, new LockApplication());
        server.setLogLevel(Level.FINER);
        server.init();
        server.listen();
    }

}
