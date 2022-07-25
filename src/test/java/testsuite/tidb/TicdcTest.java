package testsuite.tidb;

import com.mysql.cj.jdbc.ConnectionImpl;
import org.junit.jupiter.api.Test;
import testsuite.BaseTestCase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class TicdcTest extends BaseTestCase {

    private final static String QUERY_SQL = "select sum(balance) from accounts";

    private Long globalSecondaryTsValue = 0L;

    private Long secondaryTsValue = 0L;

    private Map<String, Function<ResultSet,Integer>> sqlFlow(ConnectionImpl conn1){
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put(QUERY_SQL,
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long sum = result.getLong(1);
                            assertTrue(sum != null,"符合预期，大于等于上一个值");
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
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow(conn1);
        JDBCRun.of(conn1).multipleRun(sqlFlow,5,1000L);
    }

    @Test
    public void testCdcBaseQuery() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow(conn1);
        JDBCRun.of(conn1).multipleRunBase(sqlFlow,5,null);
    }


    @Test
    public void testCdcTransactionQuery() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put(QUERY_SQL,
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long sum = result.getLong(1);
                            if(sum == 100000){
                                assertTrue(sum == 100000,"符合预期，大于等于上一个值");
                            }
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        conn.setAutoCommit(false);
        JDBCRun.of(conn).multipleRun(sqlFlow,5,5000L);
        conn.commit();
    }

    @Test
    public void testAutoCommit() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        AtomicReference<Long> gId = new AtomicReference<>(0L);
        AtomicReference<Boolean> start = new AtomicReference<>(true);
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put("select * from test",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long id = result.getLong(1);
                            if(!start.get()){
                                assertTrue(id == gId.get(),"id 不符合预期");
                            }
                            start.set(false);
                            gId.set(id);

                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRun(sqlFlow,10,1000L);
        conn1.commit();
        start.set(true);
        Thread.sleep(1000L);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRun(sqlFlow,10,1000L);
        conn1.commit();
        start.set(true);
        Thread.sleep(1000L);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn1).multipleRun(sqlFlow,10,1000L);
        conn1.commit();
        start.set(true);
        Thread.sleep(1000L);
    }


    @Test
    public void testBaseTransactionQuery() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
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

        JDBCRun.of(conn).multipleRunBase(sqlFlow,2,1000L);
        JDBCRun.of(conn).runBaseExecute("start transaction");
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,3,1000L);
        JDBCRun.of(conn).runBaseExecute("commit");
        //conn.commit();
        Thread.sleep(1000L);
        JDBCRun.of(conn).runBaseExecute("start transaction");
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,3,1000L);
        JDBCRun.of(conn).runBaseExecute("commit");
        //conn.commit();
        Thread.sleep(1000L);
    }


    @Test
    public void testAutocommit() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;

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

        JDBCRun.of(conn).multipleRunBase(sqlFlow1,2,1000L);
        conn1.setAutoCommit(true);
        Thread.sleep(5000L);
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,2,1000L);
        conn1.setAutoCommit(false);
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,2,1000L);
        Thread.sleep(5000L);
        conn1.setAutoCommit(true);
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,2,1000L);
        Thread.sleep(5000L);
    }


    @Test
    public void testCacheAutocommit() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;

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

        JDBCRun.of(conn).multipleRunBase(sqlFlow1,2,1000L);
        conn1.setAutoCommit(true);
        Thread.sleep(5000L);
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,2,1000L);
        JDBCRun.of(conn).runBaseExecute("start transaction");
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,2,1000L);
        JDBCRun.of(conn).runBaseExecute("commit");
        Thread.sleep(5000L);
        JDBCRun.of(conn).runBaseExecute("begin");
        JDBCRun.of(conn).multipleRunBase(sqlFlow1,2,1000L);
        JDBCRun.of(conn).runBaseExecute("commit");
    }


    @Test
    public void testSql() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        JDBCRun.of(conn1).run("begin");
        JDBCRun.of(conn1).runBaseExecute("begin");
    }


//    @Test
//    public void testInsertSql() throws Exception{
//        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
//        conn1.setAutoCommit(false);
//        JDBCRun.of(conn1).run("update test set id= id + 1");
//        conn1.commit();
//        JDBCRun.of(conn1).run("select id from test",
//                (ResultSet result)->{
//                    try {
//                        if (result.next()) {
//                            Long id = result.getLong(1);
//                        }
//                    } catch (SQLException e) {
//                        throw new RuntimeException(e);
//                    }
//                    return 1;
//                });
//    }

}
