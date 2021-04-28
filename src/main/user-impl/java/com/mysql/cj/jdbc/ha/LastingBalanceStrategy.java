package com.mysql.cj.jdbc.ha;

import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;

import java.lang.reflect.InvocationHandler;
import java.sql.SQLException;
import java.util.*;

public class LastingBalanceStrategy implements BalanceStrategy {

    public LastingBalanceStrategy() {

    }

    @Override
    public JdbcConnection pickConnection(InvocationHandler proxy, List<String> configuredHosts, Map<String, JdbcConnection> liveConnections, long[] responseTimes, int numRetries) throws SQLException {
        int numHosts = configuredHosts.size();
        List<String> whiteList = new ArrayList<>(numHosts);
        whiteList.addAll(configuredHosts);
        Map<String, Long> blackList = ((LoadBalancedConnectionProxy) proxy).getGlobalBlacklist();
        whiteList.removeAll(blackList.keySet());
        String hostPortSpec = whiteList.get(0);
        ConnectionImpl conn = ((LoadBalancedConnectionProxy) proxy).createConnectionForHost(hostPortSpec);
        return conn;
    }

    @Override
    public JdbcConnection pickConnection(InvocationHandler proxy, List<String> configuredHosts, Map<String, JdbcConnection> liveConnections, long[] responseTimes, int numRetries, String hostPortPair) throws SQLException {
        int numHosts = configuredHosts.size();
        List<String> whiteList = new ArrayList<>(numHosts);
        whiteList.addAll(configuredHosts);
        Map<String, Long> blackList = ((LoadBalancedConnectionProxy) proxy).getGlobalBlacklist();
        whiteList.removeAll(blackList.keySet());
        ConnectionImpl conn = null;
        conn = ((LoadBalancedConnectionProxy) proxy).createConnectionForHost(hostPortPair);
        return conn;
    }

}
