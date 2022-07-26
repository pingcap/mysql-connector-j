package testsuite.tidb;

import com.mysql.cj.jdbc.ConnectionImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import testsuite.BaseTestCase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class TicdcTest extends BaseTestCase {

    private Long timeRun = 500L;

    private Integer count = 5;

    ExecutorService executorService = Executors.newFixedThreadPool(10);

    private Map<String, Function<ResultSet,Integer>> sqlFlow(AtomicReference<Long> gId,AtomicReference<Boolean> start){
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put("select * from test",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long id = result.getLong(1);
                            System.out.println(id + "--" + gId);
                            if(start.get()){
                                gId.set(id);
                            }
                            assertTrue(id.equals(gId.get()),"id 不符合预期");
                            start.set(false);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        return sqlFlow;
    }

    private Map<String, Function<ResultSet,Integer>> sqlFlow(){
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put("select * from test",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long id = result.getLong(1);
                            System.out.println(id);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        return sqlFlow;
    }

    private Map<String, Function<ResultSet,Integer>> variablesSqlFlow(){
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put("select * from test",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long id = result.getLong(1);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        sqlFlow.put("show variables like 'tidb_snapshot'",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            String value = result.getString("Value");
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        Map<String, Function<ResultSet,Integer>> sqlFlow1 = new HashMap<>();
        sqlFlow1.put("show variables like 'autocommit'",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            String  val= result.getString("Value") ;
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        sqlFlow1.put("select * from test",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long id = result.getLong(1);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        return sqlFlow;
    }

    @Test
    public void testCdcQuery() throws Exception{
        ConnectionImpl conn1 = getSnapshotConn();
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow();
        JDBCRun.of(conn1).multipleRun(sqlFlow,10,timeRun);
        conn1.close();
    }

    @Test
    public void testCdcBaseQuery() throws Exception{
        ConnectionImpl conn1 = getSnapshotConn();
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow();
        JDBCRun.of(conn1).multipleRunBase(sqlFlow,count,null);
        conn1.close();
    }


    @Test
    public void testCdcTransactionQuery() throws Exception{
        ConnectionImpl conn1 = getSnapshotConn();
        AtomicReference<Long> gId = new AtomicReference<>(0L);
        AtomicReference<Boolean> start = new AtomicReference<>(true);
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow(gId,start);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRun(sqlFlow,count,timeRun);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRun(sqlFlow,count,timeRun);
        conn1.commit();
        start.set(true);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRun(sqlFlow,count,timeRun);
        conn1.commit();
        start.set(true);
        conn1.close();
    }

    @Test
    public void testAutoCommit() throws Exception{
        ConnectionImpl conn1 = getSnapshotConn();
        AtomicReference<Long> gId = new AtomicReference<>(0L);
        AtomicReference<Boolean> start = new AtomicReference<>(true);
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow(gId,start);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRun(sqlFlow,count,timeRun);
        conn1.commit();
        start.set(true);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRun(sqlFlow,count,timeRun);
        conn1.commit();
        start.set(true);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRun(sqlFlow,count,timeRun);
        conn1.commit();
        start.set(true);
        conn1.close();
    }


    @Test
    public void testBaseTransactionQuery() throws Exception{
        ConnectionImpl conn1 = getSnapshotConn();

        AtomicReference<Long> gId = new AtomicReference<>(0L);
        AtomicReference<Boolean> start = new AtomicReference<>(true);
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow(gId,start);
        JDBCRun.of(conn1).multipleRunBase(sqlFlow,count,timeRun);
        JDBCRun.of(conn1).runBaseExecute("start transaction");
        JDBCRun.of(conn1).multipleRunBase(sqlFlow,count,timeRun);
        JDBCRun.of(conn1).runBaseExecute("commit");
        start.set(true);
        JDBCRun.of(conn1).runBaseExecute("start transaction");
        JDBCRun.of(conn1).multipleRunBase(sqlFlow,count,timeRun);
        JDBCRun.of(conn1).runBaseExecute("commit");
        start.set(true);
        conn1.close();
    }


    @Test
    public void testAutocommit() throws Exception{
        ConnectionImpl conn1 = getSnapshotConn();
        AtomicReference<Long> gId = new AtomicReference<>(0L);
        AtomicReference<Boolean> start = new AtomicReference<>(true);
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow(gId,start);
        Map<String, Function<ResultSet,Integer>> sqlFlow1 = sqlFlow();
        JDBCRun.of(conn1).multipleRunBase(sqlFlow1,count,timeRun);
        conn1.setAutoCommit(true);
        JDBCRun.of(conn1).multipleRunBase(sqlFlow1,count,timeRun);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRunBase(sqlFlow,count,timeRun);
        conn1.commit();
        start.set(true);
        conn1.setAutoCommit(true);
        JDBCRun.of(conn1).multipleRunBase(sqlFlow1,count,timeRun);
        conn1.close();
    }


    @Test
    public void testCacheAutocommit() throws Exception{
        ConnectionImpl conn1 = getSnapshotConn();
        AtomicReference<Long> gId = new AtomicReference<>(0L);
        AtomicReference<Boolean> start = new AtomicReference<>(true);
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow(gId,start);
        Map<String, Function<ResultSet,Integer>> sqlFlow1 = sqlFlow();
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,count,timeRun);
        conn1.setAutoCommit(true);
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,count,timeRun);
        JDBCRun.of(conn).runBaseExecute("start transaction");
        JDBCRun.of(conn).multipleRunBase(sqlFlow,count,timeRun);
        JDBCRun.of(conn).runBaseExecute("commit");
        start.set(true);
        JDBCRun.of(conn).runBaseExecute("begin");
        JDBCRun.of(conn).multipleRunBase(sqlFlow,count,timeRun);
        JDBCRun.of(conn).runBaseExecute("commit");
        start.set(true);
        conn1.close();
    }


    @Test
    public void testSql() throws Exception{
        ConnectionImpl conn1 = getSnapshotConn();
        JDBCRun.of(conn1).run("begin");
        JDBCRun.of(conn1).runBaseExecute("begin");
        conn1.close();
    }


    private ConnectionImpl getSnapshotConn(){
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        Properties info = new Properties();
        Properties properties = conn1.getProperties();
        final String[] url = new String[]{"jdbc:mysql://"};
        String jdbcUrl = buildUrlProperties(conn1, properties, info, url);
        if(!properties.containsKey("useTicdcACID")){
            if(url[0].endsWith("?")){
                url[0] += "useTicdcACID=true";
            }else {
                url[0] += "&useTicdcACID=true";
            }
        }
        try {
            return (ConnectionImpl) this.getConnectionWithProps(jdbcUrl,info);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String buildUrlProperties(ConnectionImpl conn1,Properties properties,Properties info,final String[] url){
        try {
            url[0] += conn1.getHostPortPair() + "/" + conn1.getDatabase() + "?";
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        properties.entrySet().forEach(entry ->{
            if("user".equals(entry.getKey())){
                info.put(entry.getKey(),entry.getValue());
            }else if("password".equals(entry.getKey())){
                info.put(entry.getKey(),entry.getValue());
            }else if(!"port".equals(entry.getKey()) && !"dbname".equals(entry.getKey())
                    && !"host".equals(entry.getKey())&& !"useTicdcACID".equals(entry.getKey())){
                if(url[0].endsWith("?")){
                    url[0] += entry.getKey() + "=" + entry.getValue();
                }else {
                    url[0] += "&"+ entry.getKey() + "=" + entry.getValue();
                }
            }
        });
        return url[0];
    }

    private ConnectionImpl getBaseConn(){
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        Properties info = new Properties();
        Properties properties = conn1.getProperties();
        final String[] url = new String[]{"jdbc:mysql://"};
        String jdbcUrl = buildUrlProperties(conn1, properties, info, url);
        if(!properties.containsKey("useTicdcACID")){
            if(jdbcUrl.endsWith("?")){
                jdbcUrl += "useTicdcACID=false";
            }else {
                jdbcUrl += "&useTicdcACID=false";
            }
        }else {
            if(jdbcUrl.endsWith("?")){
                jdbcUrl += "useTicdcACID=false";
            }else {
                jdbcUrl += "&useTicdcACID=false";
            }
        }
        try {
            return  (ConnectionImpl) this.getConnectionWithProps(jdbcUrl,info);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ScheduledThreadPoolExecutor executor;

    private static final AtomicInteger threadId = new AtomicInteger();

    ConnectionImpl baseConn = null;

    @AfterEach
    public void endTest(){
        this.executor.shutdown();
    }

    @BeforeEach
    public void initData() throws Exception{
        baseConn = getBaseConn();
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
        this.executor.setKeepAliveTime(5000*2, TimeUnit.MILLISECONDS);
        this.executor.allowCoreThreadTimeOut(true);
        this.executor.scheduleWithFixedDelay(
                this::reload, 0, 5000, TimeUnit.MILLISECONDS);

    }

    private void reload(){
        try {
            if(baseConn != null){
                baseConn.setAutoCommit(false);
                JDBCRun.of(baseConn).run("update test set id= id + 1");
                baseConn.commit();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}
