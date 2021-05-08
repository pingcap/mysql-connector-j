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
        return null;
    }

    @Override
    public JdbcConnection pickConnection(InvocationHandler proxy, String hostPortPair) throws SQLException {
        return ((LoadBalancedConnectionProxy) proxy).createConnectionForHost(hostPortPair);
    }

}
