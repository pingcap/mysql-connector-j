package testsuite.tidb;

import com.mysql.cj.jdbc.ConnectionImpl;
import org.junit.jupiter.api.Test;
import testsuite.BaseTestCase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class TicdcTest extends BaseTestCase {

    private final static String QUERY_SQL = "select sum(balance) from accounts";

    private Long globalSecondaryTsValue = 0L;

    private Long secondaryTsValue = 0L;

    private void cdcValueAssert(ConnectionImpl conn1){
        Long globalSecondaryTs = conn1.getTicdc().getGlobalSecondaryTs().get();
        Long secondaryTs = conn1.getSecondaryTs();
        String cfName = conn1.getTicdc().getTicdcCFname();
        assertTrue(cfName != null,"TicdcCFname 不符合预期");
        assertTrue(globalSecondaryTs != 0,  "globalSecondaryTs不符合预期");
        assertTrue(secondaryTs != 0,  "secondaryTs 不符合预期");
        globalSecondaryTsValue = globalSecondaryTs;
        secondaryTsValue = secondaryTs;

    }

    private Map<String, Function<ResultSet,Integer>> sqlFlow(ConnectionImpl conn1){
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
                        cdcValueAssert(conn1);
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
        try {
            JDBCRun.of(conn1).multipleRun(sqlFlow,20,5000L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCdcBaseQuery() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        Map<String, Function<ResultSet,Integer>> sqlFlow = sqlFlow(conn1);
        try {
            JDBCRun.of(conn1).multipleRunBase(sqlFlow,20,null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
                        cdcValueAssert(conn1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        try {
            conn.setAutoCommit(false);
            JDBCRun.of(conn).multipleRun(sqlFlow,20,5000L);
            conn.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCdcUserTransactionQuery() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put("begin",null);

        try {
            JDBCRun.of(conn1).runBase(sqlFlow);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCdcBigTransactionQuery() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        conn1.setAutoCommit(false);
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put("select * from test",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long id = result.getLong(1);
                        }
                        cdcValueAssert(conn1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        try {
            JDBCRun.of(conn).multipleRun(sqlFlow,5,5000L);
            conn.commit();
            Thread.sleep(5000L);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
                        cdcValueAssert(conn1);
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
                        cdcValueAssert(conn1);
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
                        cdcValueAssert(conn1);
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
                        cdcValueAssert(conn1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });

        try {
            while (true){
                JDBCRun.of(conn).multipleRunBase(sqlFlow,2,1000L);
                JDBCRun.of(conn).runBaseExecute("start transaction");
                for (int i=0;i<10;i++){
                    JDBCRun.of(conn).multipleRunBase(sqlFlow1,10,2000L);
                    JDBCRun.of(conn).runBaseExecute("commit");
                    //conn.commit();
                    Thread.sleep(5000L);
                }

            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
                        cdcValueAssert(conn1);
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
                        cdcValueAssert(conn1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });

        try {
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


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
                        cdcValueAssert(conn1);
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
                        cdcValueAssert(conn1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });

        try {
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


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testSql() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;

        try {
            JDBCRun.of(conn1).run("begin");
            JDBCRun.of(conn1).runBaseExecute("begin");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testInsertSql() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;

        try {
            conn1.setAutoCommit(false);
            JDBCRun.of(conn1).run("update test set id= id + 1");
            conn1.commit();
            JDBCRun.of(conn1).run("select id from test",
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

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
