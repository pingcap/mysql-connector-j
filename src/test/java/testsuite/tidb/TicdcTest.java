package testsuite.tidb;

import com.mysql.cj.jdbc.ConnectionImpl;
import org.junit.jupiter.api.Test;
import testsuite.BaseTestCase;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TicdcTest extends BaseTestCase {

    @Test
    public void testCdcQuery() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put("select sum(balance) from accounts",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long sum = result.getLong(1);
                            if(sum == 100000){
                                assertTrue(sum == 100000,"符合预期，大于等于上一个值");
                            }
                        }
                        Long globalSecondaryTs = conn1.getTicdc().getGlobalSecondaryTs().get();
                        Long secondaryTs = conn1.getSecondaryTs();
                        assertTrue(globalSecondaryTs != 0,  "globalSecondaryTs 符合预期");
                        assertTrue(secondaryTs != 0,  "secondaryTs 符合预期");
                        assertEquals(globalSecondaryTs.equals(secondaryTs), true, "secondaryTs一致 ");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        try {
            for (int i=0;i<20;i++){
                //conn.setAutoCommit(false);
                JDBCRun.of(conn).run(sqlFlow);
                //conn.commit();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testCdcTransactionQuery() throws Exception{
        ConnectionImpl conn1 = (ConnectionImpl) this.conn;
        Map<String, Function<ResultSet,Integer>> sqlFlow = new HashMap<>();
        sqlFlow.put("select sum(balance) from accounts",
                (ResultSet result)->{
                    try {
                        if (result.next()) {
                            Long sum = result.getLong(1);
                            if(sum == 100000){
                                assertTrue(sum == 100000,"符合预期，大于等于上一个值");
                            }
                        }
                        Long globalSecondaryTs = conn1.getTicdc().getGlobalSecondaryTs().get();
                        Long secondaryTs = conn1.getSecondaryTs();
                        assertTrue(globalSecondaryTs != 0,  "globalSecondaryTs 符合预期");
                        assertTrue(secondaryTs != 0,  "secondaryTs 符合预期");
                        assertEquals(globalSecondaryTs.equals(secondaryTs), true, "secondaryTs一致 ");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return 1;
                });
        try {
            for (int i=0;i<20;i++){
                conn.setAutoCommit(false);
                JDBCRun.of(conn).run(sqlFlow);
                conn.commit();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
