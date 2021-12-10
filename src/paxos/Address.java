package paxos;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.net.*;
import java.util.*;
import java.util.regex.Pattern;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Address implements Comparable<Address>, Serializable, Copyable {

    public static final int PORT = 5000;

    @Getter
    private final String hostname;
    @Getter
    @EqualsAndHashCode.Include
    private final InetSocketAddress inetSocketAddress;

    public static String verifyIPv4(String ipv4Addr) {
        ipv4Addr = ipv4Addr.trim();
        if (!Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+").matcher(ipv4Addr).matches()) {
            throw new IllegalArgumentException(String.format("Illegal IPv4 address: %s", ipv4Addr));
        }
        return ipv4Addr;
    }

    public static Address parseIPv4(String address) {
        address = address.trim();
        String[] arr = address.split(":");
        if (arr.length == 1) {
            return new Address(arr[0]);
        } else if (arr.length == 2) {
            return new Address(arr[0], Integer.parseInt(arr[1]));
        } else {
            throw new IllegalArgumentException(String.format("Illegal IPv4 address: %s", address));
        }
    }

    private Address(String hostname) {
        this(hostname, PORT);
    }

    public Address(String hostname, int port) {
        this(new InetSocketAddress(verifyIPv4(hostname), port));
    }

    public Address(InetSocketAddress inetSocketAddress) {
        this.inetSocketAddress = inetSocketAddress;
        this.hostname = String.format(
                "%s:%d", inetSocketAddress.getAddress().getHostAddress(), inetSocketAddress.getPort());
        this.stringRep = String.format("%s(%s)", this.getClass().getSimpleName(), hostname);
    }

    @Override
    public int compareTo(Address o) {
        byte[] thisBytes = this.inetSocketAddress.getAddress().getAddress();
        byte[] otherBytes = o.inetSocketAddress.getAddress().getAddress();
        if (thisBytes.length != 4 || otherBytes.length != 4) {
            throw new IllegalStateException("Address must be an IPv4");
        }
        for (int i = 0; i < 4; ++i) {
            if (thisBytes[i] != otherBytes[i]) {
                return thisBytes[i] - otherBytes[i];
            }
        }
        return this.inetSocketAddress.getPort() - o.inetSocketAddress.getPort();
    }

    private final String stringRep;

    @Override
    public String toString() {
        return stringRep;
    }

    @Override
    public Address immutableCopy() {
        return this;
    }

    public static Address[] getServerAddresses(String configPath) throws FileNotFoundException {
        try (Scanner s = new Scanner(new File(configPath))) {
            List<Address> addrs = new ArrayList<>();
            while (s.hasNextLine()) {
                String hostaddr = s.nextLine();
                addrs.add(parseIPv4(hostaddr));
            }
            return addrs.toArray(new Address[0]);
        }
    }

}
