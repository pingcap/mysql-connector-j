package com.mysql.cj.jdbc;

import java.util.concurrent.ConcurrentHashMap;

public class AllConnections {

    private static ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();

    public ConcurrentHashMap<String, Integer> getMap() {
        return map;
    }

    public void setMap(ConcurrentHashMap<String, Integer> map) {
        this.map = map;
    }

    private static AllConnections allConnections;

    private AllConnections() {
    }

    public static synchronized AllConnections getInstance() {
        if (allConnections == null) {
            allConnections = new AllConnections();
        }
        return allConnections;
    }

}
