package testsuite.tidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
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

    public void run(Map<String,Function<ResultSet,Integer>> sqlFlow){
        sqlFlow.forEach((k,v)->{
            run(k, v);
        });
    }

    public void multipleRun(Map<String,Function<ResultSet,Integer>> sqlFlow,int count){
        for(int i=0;i<count;i++){
            run(sqlFlow);
        }
    }

    public void runBase(String sql, Function<ResultSet,Integer> fun){
        Statement ps = null;
        ResultSet result = null;
        try {
            ps = conn.createStatement();
            result = ps.executeQuery(sql);
            if(fun != null){
                fun.apply(result);
            }
        } catch (SQLException e) {
            log.error("Statement error",e);
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

    public void runBase(Map<String,Function<ResultSet,Integer>> sqlFlow){
        sqlFlow.forEach((k,v)->{
            runBase(k, v);
        });
    }

    public void multipleRunBase(Map<String,Function<ResultSet,Integer>> sqlFlow,int count){
        for(int i=0;i<count;i++){
            runBase(sqlFlow);
        }
    }
}
