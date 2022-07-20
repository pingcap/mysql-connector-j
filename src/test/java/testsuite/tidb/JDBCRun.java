package testsuite.tidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class JDBCRun {
    private static final Logger log = LoggerFactory.getLogger(JDBCRun.class);

    private Connection conn;

    public JDBCRun(){

    }

    public JDBCRun(Connection conn){
        this.conn = conn;
    }

    public static JDBCRun create(){
        return new JDBCRun();
    }

    public static JDBCRun of(Connection conn){
        return new JDBCRun(conn);
    }

    public void run(String sql, Function<ResultSet,Integer> fun){
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet result = ps.executeQuery();
            fun.apply(result);
        }catch (SQLException e) {
            log.error("PrepareTest error",e);
            throw new RuntimeException(e);
        }
    }

    public void runPrepared(String sql, Function<ResultSet,Integer> fun){
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet result = ps.executeQuery();
            fun.apply(result);
        }catch (SQLException e) {
            log.error("PrepareTest error",e);
            throw new RuntimeException(e);
        }
    }


    public void run(Map<String,Function<ResultSet,Integer>> sqlFlow){
        sqlFlow.forEach((k,v)->{
            try (PreparedStatement ps = conn.prepareStatement(k)) {
                ResultSet result = ps.executeQuery();
                v.apply(result);
            }catch (SQLException e) {
                log.error("run error",e);
                throw new RuntimeException(e);
            }
        });
    }

    public void runPrepared(Map<String,Function<ResultSet,Integer>> sqlFlow){
        sqlFlow.forEach((k,v)->{
            try (PreparedStatement ps = conn.prepareStatement(k)) {
                ResultSet result = ps.executeQuery();
                v.apply(result);
            }catch (SQLException e) {
                log.error("run error",e);
                throw new RuntimeException(e);
            }
        });
    }

    public void runBase(String sql, Function<ResultSet,Integer> fun){
        Statement ps = null;
        ResultSet result = null;
        try {
            ps = conn.createStatement();
            result = ps.executeQuery(sql);
            fun.apply(result);
        } catch (SQLException e) {
            log.error("PrepareTest error",e);
            throw new RuntimeException(e);
        }finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException sqlEx) { } // ignore
                result = null;
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException sqlEx) { } // ignore
                ps = null;
            }
        }
    }
}
