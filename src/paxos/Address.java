package paxos;

import java.net.*;
import java.util.Enumeration;

public class Address implements Comparable<Address> {

    public static final int PORT = 5000;
    private final String hostname;
    private final InetAddress inetAddress;
    private final InetSocketAddress inetSocketAddress;

    public Address(String hostname) throws UnknownHostException {
        this.hostname = hostname;
        this.inetAddress = InetAddress.getByName(hostname);
        this.inetSocketAddress = new InetSocketAddress(inetAddress, PORT);
    }

    public String getHostname() {
        return hostname;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public InetSocketAddress getInetSocketAddress() {
        return inetSocketAddress;
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

}
