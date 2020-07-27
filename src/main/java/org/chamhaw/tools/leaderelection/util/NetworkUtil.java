package org.chamhaw.tools.leaderelection.util;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import org.apache.commons.lang3.StringUtils;

/**
 * @author hao.zhang
 * @date 2018/5/11
 */
@Slf4j
public class NetworkUtil {
    @Getter
    private static final String localIp = getCurrentIpAddress(null);

    @Getter
    private static final String localhostWithPort = NetworkUtil.getLocalIp() + ":" + System.getProperty("server.port", "8080");

    /**
     *
     * @param netInterface @Nullable default is the first interface
     * @return
     */
    public static String getCurrentIpAddress(String netInterface) {
        Enumeration<NetworkInterface> en;
        try {
            en = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            logger.error("cannot get the ip address with the interface:{}", netInterface);
            return StringUtils.EMPTY;
        }
        while (en.hasMoreElements()) {
            NetworkInterface i = en.nextElement();
            if (StringUtils.isNotBlank(netInterface) && !StringUtils.equalsIgnoreCase(i.getName(), netInterface)){
                continue;
            }
            for (Enumeration<InetAddress> en2 = i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr = en2.nextElement();
                if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress() && addr instanceof Inet4Address) {
                        return addr.getHostAddress();
                }
            }
        }
        return StringUtils.EMPTY;
    }
}
