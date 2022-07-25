
/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.tidb.snapshot;


import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.tidb.jdbc.TidbCdcOperate;

import java.sql.Connection;
import java.sql.Driver;

import java.sql.PreparedStatement;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Monitor tidb cdc SecondaryTs value
 *
 */
public class Monitor {
    private Ticdc ticdc = new Ticdc();

    private Map<Ticdc,String> ticdcMap = new ConcurrentHashMap<>();
    private String url;

    private Properties info;

    private AtomicReference<java.sql.Connection> conn = new AtomicReference<>();


    private AtomicReference<PreparedStatement> preparedStatement = new AtomicReference<>();


    private ScheduledThreadPoolExecutor executor;

    private Driver driver;


    private Properties properties;

    private final Lock connLock = new ReentrantLock();

    private static final AtomicInteger threadId = new AtomicInteger();

    private static final String TIDB_TICDC_ACID_INTERVAL_KEY = "ticdcACIDInterval";

    private static final String TIDB_USE_TICDC_ACID_KEY = "useTicdcACID";

    private static final String TIDB_TICDC_CF_NAME_KEY = "ticdcCFname";

    private final AtomicLong ticdcACIDInterval = new AtomicLong(100);


    private String ticdcCFname;

    private String ticdcACIDIntervalValue;

    private String useTicdcACID;


    public Monitor(Driver driver,String url,Properties info,ScheduledThreadPoolExecutor executor){
        this.driver = driver;
        this.url = url;
        this.info = info;
        this.executor = executor;
        createExecutor();
    }

    public Monitor(Driver driver){
        this.driver = driver;
    }

    public static Monitor of(Driver driver,String url,Properties info,ScheduledThreadPoolExecutor executor){
        return new Monitor(driver,url,info,executor);
    }

    public static Monitor of(Driver driver){
        return new Monitor(driver);
    }

    public Monitor setInfo(String url,Properties info){
        this.url = url;
        this.info = info;
        Ticdc ticdc = new Ticdc();
        this.ticdc = ticdc;
        ticdcMap.put(ticdc,url);
        parser();

        createExecutor();
        registerDestroy();
        return this;
    }

    /**
     * register Destroy close conn and ps
     */
    public void registerDestroy(){
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                close();
            }
        });
    }

    /**
     * parser url properties
     */
    private void parser(){
        if(ConnectionUrl.acceptsUrl(this.url)){
            ConnectionUrl connStr = ConnectionUrl.getConnectionUrlInstance(this.url, this.info);
            this.properties = connStr.getConnectionArgumentsAsProperties();

            this.ticdcCFname = properties.getProperty(TIDB_TICDC_CF_NAME_KEY);
            this.ticdcACIDIntervalValue = properties.getProperty(TIDB_TICDC_ACID_INTERVAL_KEY);
            this.useTicdcACID = properties.getProperty(TIDB_USE_TICDC_ACID_KEY);

            if(this.ticdcACIDIntervalValue != null){
                ticdcACIDInterval.set(Long.parseLong(this.ticdcACIDIntervalValue));
            }

            if(this.ticdcCFname != null){
                ticdc.setTicdcCFname(this.ticdcCFname);
            }

            if(this.useTicdcACID != null){
                this.ticdc.setUseTicdcACID(this.useTicdcACID);
            }
        }
    }

    private Boolean isRun(){
        if(this.useTicdcACID == null){
            return false;
        }
        if(!"true".equals(this.useTicdcACID)){
            return false;
        }
        return true;
    }

    /**
     * create ScheduledThreadPoolExecutor get  SecondaryTs value
     */
    public void createExecutor(){
        if(!isRun()){
            return;
        }
        if(this.executor != null){
            return;
        }
        String executorName = "reload-Thread-" + threadId.getAndIncrement();
        this.executor =
                new ScheduledThreadPoolExecutor(
                        Runtime.getRuntime().availableProcessors(),
                        (runnable) -> {
                            Thread newThread = new Thread(runnable);
                            newThread.setName(executorName);
                            newThread.setDaemon(true);
                            return newThread;
                        });
        this.executor.setKeepAliveTime(ticdcACIDInterval.get()*2, TimeUnit.MILLISECONDS);
        this.executor.allowCoreThreadTimeOut(true);
        this.executor.scheduleWithFixedDelay(
                this::reload, 0, ticdcACIDInterval.get(), TimeUnit.MILLISECONDS);
    }

    public Ticdc get(){
        return ticdc;
    }

    /**
     * create jdbc connect
     * concurrency create add lock
     */
    private void connect(){
        if(this.url == null){
            return;
        }
        if("".equals(this.url)){
            return;
        }
        try {
            if(this.conn.get() == null){
                if(connLock.tryLock()){
                    Connection conn = driver.connect(this.url,this.info);
                    conn.setAutoCommit(true);
                    this.conn.set(conn);
                    connLock.unlock();
                }
            }
        }catch (SQLException e) {
            connLock.unlock();
            throw new RuntimeException(e);
        }
    }

    /**
     * set Global SecondaryTs
     */
    public void setGlobalSecondaryTs(){
        try {
            String secondaryTs = TidbCdcOperate.of((ConnectionImpl) conn.get(),ticdc).setPreparedStatement(preparedStatement).getSnapshot();
            if(secondaryTs != null){
                Long secondaryTsValue = Long.parseLong(secondaryTs);
                this.ticdcMap.forEach((k,v)->{
                    if(k.getGlobalSecondaryTs().get() != secondaryTsValue){
                        k.getGlobalSecondaryTs().set(Long.parseLong(secondaryTs));
                        k.getGloballasttime().set(System.currentTimeMillis());
                    }
                });


            }else {
                throw new RuntimeException("secondaryTs is null");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * monitor get SecondaryTs and set Global SecondaryTs
     */
    public void reload(){
        connect();
        if(this.conn.get() == null){
            return;
        }
        try {
            setGlobalSecondaryTs();
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }


    /**
     * close jdbc preparedStatement conn object
     * @return
     */
    public Monitor close(){
        try {
            if(this.preparedStatement.get() != null){
               this.preparedStatement.get().close();
            }
            if(this.conn.get() != null){
                this.conn.get().close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }
}
