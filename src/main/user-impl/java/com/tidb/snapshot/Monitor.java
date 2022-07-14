package com.tidb.snapshot;


import com.mysql.cj.jdbc.ConnectionImpl;
import com.tidb.jdbc.TidbCdcOperate;

import java.sql.Driver;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Monitor {
    private Ticdc ticdc = new Ticdc();

    private String url;

    private Properties info;

    private AtomicReference<java.sql.Connection> conn = new AtomicReference<>();

    private ScheduledThreadPoolExecutor executor;

    private Driver driver;

    private Lock connLock = new ReentrantLock();

    private static final AtomicInteger threadId = new AtomicInteger();


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
        createExecutor();
        return this;
    }

    public void createExecutor(){
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
        this.executor.setKeepAliveTime(10000, TimeUnit.MILLISECONDS);
        this.executor.allowCoreThreadTimeOut(true);
        this.executor.scheduleWithFixedDelay(
                this::reload, 0, 10000, TimeUnit.MILLISECONDS);
    }

    public Ticdc get(){
        return ticdc;
    }

    private void connect(){
        try {
            if(this.conn.get() == null){
                if(connLock.tryLock()){
                    this.conn.set(driver.connect(this.url,this.info));
                    connLock.unlock();
                }
            }
        }catch (SQLException e) {
            connLock.unlock();
            throw new RuntimeException(e);
        }
    }

    public void setGlobalSecondaryTs(){
        try {
            String secondaryTs = TidbCdcOperate.of((ConnectionImpl) conn.get(),ticdc).getSnapshot();
            if(secondaryTs != null){
                Long secondaryTsValue = Long.parseLong(secondaryTs);
                if(ticdc.getGlobalSecondaryTs().get() != secondaryTsValue){
                    this.ticdc.getGlobalSecondaryTs().set(Long.parseLong(secondaryTs));
                    this.ticdc.getGloballasttime().set(System.currentTimeMillis());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void reload(){
        if(this.url == null){
            return;
        }
        if("".equals(this.url)){
            return;
        }
        connect();
        if(this.conn.get() == null){
            return;
        }
        setGlobalSecondaryTs();
    }
}
