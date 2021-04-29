package com.mysql.cj.jdbc;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class AllConnections {

    private static HashMap<String, Integer> map = new HashMap<>();

    public HashMap<String, Integer> getMap() {
        return map;
    }

    public void setMap(HashMap<String, Integer> map) {
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
