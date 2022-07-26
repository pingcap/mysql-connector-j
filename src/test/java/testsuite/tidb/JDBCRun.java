package testsuite.tidb;

import java.sql.*;
import java.util.Map;
import java.util.function.Function;

public class JDBCRun {
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
            throw new RuntimeException(e);
        }
    }

    public void run(String sql){
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            Boolean state = ps.execute();
        }catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void run(Map<String,Function<ResultSet,Integer>> sqlFlow){
        sqlFlow.forEach((k,v)->{
            run(k, v);
        });
    }

    public void multipleRun(Map<String,Function<ResultSet,Integer>> sqlFlow,int count,Long timeRun){
        for(int i=0;i<count;i++){
            run(sqlFlow);
            if(timeRun != null){
                try {
                    Thread.sleep(timeRun);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
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
            throw new RuntimeException(e);
        }finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException sqlEx) {
                    throw new RuntimeException(sqlEx);
                }
                result = null;
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException sqlEx) {
                    throw new RuntimeException(sqlEx);
                }
                ps = null;
            }
        }
    }

    public void runBaseExecute(String sql){
        Statement ps = null;
        ResultSet result = null;
        try {
            ps = conn.createStatement();
            Boolean state = ps.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException sqlEx) {
                    throw new RuntimeException(sqlEx);
                }
                result = null;
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException sqlEx) {
                    throw new RuntimeException(sqlEx);
                }
                ps = null;
            }
        }
    }

    public void runBase(Map<String,Function<ResultSet,Integer>> sqlFlow){
        sqlFlow.forEach((k,v)->{
            runBase(k, v);
        });
    }

    public void multipleRunBase(Map<String,Function<ResultSet,Integer>> sqlFlow,int count,Long timeRun){
        for(int i=0;i<count;i++){
            runBase(sqlFlow);
            try {
                if(timeRun != null){
                    Thread.sleep(timeRun);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
