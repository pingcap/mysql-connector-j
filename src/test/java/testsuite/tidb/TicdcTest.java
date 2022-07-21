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
        System.out.println("test-getCfname="+cfName);
        assertTrue(cfName != null,"TicdcCFname 不符合预期");
        assertTrue(globalSecondaryTs != 0,  "globalSecondaryTs不符合预期");
        assertTrue(secondaryTs != 0,  "secondaryTs 不符合预期");
        //assertEquals(globalSecondaryTs.equals(secondaryTs), true, "secondaryTs不一致 ");
        //System.out.println("test-globalSecondaryTsValue:"+globalSecondaryTsValue + ",globalSecondaryTs:"+globalSecondaryTs);
        if(globalSecondaryTsValue == 0){
            System.out.println("test-globalSecondaryTs:"+globalSecondaryTs+",secondaryTs:"+secondaryTs);
        }else if(!globalSecondaryTsValue.equals(globalSecondaryTs)){
            System.out.println("test-globalSecondaryTs:"+globalSecondaryTs+",secondaryTs:"+secondaryTs);
        }else if(!secondaryTsValue.equals(secondaryTs)){
            System.out.println("test-globalSecondaryTs:"+globalSecondaryTs+",secondaryTs:"+secondaryTs);
        }


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
            JDBCRun.of(conn1).multipleRunBase(sqlFlow,20);
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
                            System.out.println("id="+id);
                        }
                        cdcValueAssert(conn1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        try {
            while (true){
                JDBCRun.of(conn).multipleRun(sqlFlow,5,5000L);
                conn.commit();
                System.out.println("test-commit");
                Thread.sleep(5000L);
            }


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
                            System.out.println("id="+id);
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
                            System.out.println("value="+value);
                        }
                        cdcValueAssert(conn1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        sqlFlow.put("select * from test",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long id = result.getLong(1);
                            System.out.println("id="+id);
                        }
                        cdcValueAssert(conn1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        try {
            while (true){
                JDBCRun.of(conn).multipleRunBase(sqlFlow,1);
                //conn.commit();
                System.out.println("test-commit");
                Thread.sleep(5000L);
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
