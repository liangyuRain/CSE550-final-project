package paxos;

import lombok.Data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.net.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;

@Data
public class Address implements Comparable<Address>, Serializable, Copyable {

    public static final int PORT = 5000;
    private final String hostname;
    private final InetAddress inetAddress;
    private final InetSocketAddress inetSocketAddress;

    public Address(String hostname) throws UnknownHostException {
        this.hostname = hostname;
        this.inetAddress = InetAddress.getByName(hostname);
        this.inetSocketAddress = new InetSocketAddress(inetAddress, PORT);
    }

    @Override
    public int compareTo(Address o) {
        return Integer.compare(hostname.hashCode(), o.hostname.hashCode());
    }

    public static Address getLocalAddress() throws SocketException, UnknownHostException {
        Enumeration<NetworkInterface> infs = NetworkInterface.getNetworkInterfaces();
        while (infs.hasMoreElements()) {
            NetworkInterface inf = infs.nextElement();
            Enumeration<InetAddress> addrs = inf.getInetAddresses();
            while (addrs.hasMoreElements()) {
                String addr = addrs.nextElement().getHostAddress();
                if (addr.startsWith("10.")) {
                    return new Address(addr);
                }
            }
        }
        return null;
    }

    public static Address[] getServerAddresses(String configpath) throws FileNotFoundException, UnknownHostException {
        Scanner s = new Scanner(new File(configpath));
        List<Address> addrs = new ArrayList<>();
        while (s.hasNextLine()) {
            String hostaddr = s.nextLine();
            addrs.add(new Address(hostaddr));
        }
        return addrs.toArray(new Address[0]);
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", this.getClass().getSimpleName(), hostname);
    }

    @Override
    public Address immutableCopy() {
        return this;
    }

}
