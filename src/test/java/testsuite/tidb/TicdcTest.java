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

    private void cdcValueAssert(ConnectionImpl conn1){
        Long globalSecondaryTs = conn1.getTicdc().getGlobalSecondaryTs().get();
        Long secondaryTs = conn1.getSecondaryTs();
        String cfName = conn1.getTicdc().getTicdcCFname();
        System.out.println("test-getCfname="+cfName);
        assertTrue(cfName != null,"TicdcCFname 符合预期");
        assertTrue(globalSecondaryTs != 0,  "globalSecondaryTs 符合预期");
        assertTrue(secondaryTs != 0,  "secondaryTs 符合预期");
        assertEquals(globalSecondaryTs.equals(secondaryTs), true, "secondaryTs一致 ");
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
            JDBCRun.of(conn1).multipleRun(sqlFlow,20);
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
            JDBCRun.of(conn).multipleRun(sqlFlow,20);
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

}
