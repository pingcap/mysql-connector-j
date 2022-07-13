package com.tidb.jdbc;

import com.mysql.cj.jdbc.ConnectionImpl;
import com.tidb.snapshot.Ticdc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TidbCdcOperate {

    private static final String TIDB_USE_TICDC_ACID_KEY = "useTicdcACID";

    private static final String TIDB_TICDC_CF_NAME_KEY = "ticdcCFname";

    private static final String QUERY_TIDB_SNAPSHOT_SQL =
            "select `secondary_ts` from `tidb_cdc`.`syncpoint_v1` where `cf` = ? order by `primary_ts` desc limit 1";


    public ConnectionImpl connection;

    public Ticdc ticdc;

    public TidbCdcOperate(ConnectionImpl connection,Ticdc ticdc){
        this.connection = connection;
        this.ticdc = ticdc;
    }

    public static TidbCdcOperate of(ConnectionImpl connection,Ticdc ticdc){
        return new TidbCdcOperate(connection,ticdc);
    }

    public TidbCdcOperate refreshSnapshot(){
        String useTicdcACID = getTidbSnapshotParameter(TIDB_USE_TICDC_ACID_KEY,null);
        if(useTicdcACID == null){
            return this;
        }
        if(!"true".equals(useTicdcACID)){
            return this;
        }
        try {
            if(connection != null){
                setSnapshot();
            }

        }catch (SQLException e){
            System.out.println("refreshSnapshot error:"+e.getMessage());
        }
        return this;
    }

    private String getTidbSnapshotParameter(String key,String defaultValue){
        if(connection == null){
            return defaultValue;
        }
        String value = this.connection.getProperties().getProperty(key);
        if(value == null){
            value = defaultValue;
        }
        return value;
    }

    public TidbCdcOperate setSnapshot() throws SQLException{
        if(this.ticdc.getGlobalSecondaryTs().get() == 0){
            return this;
        }
        if(this.connection.getSecondaryTs() == 0){
            this.connection.getSession().setSnapshot("");
            String secondaryTs = getSnapshot();
            if(secondaryTs != null){
                Long secondaryTsValue = Long.parseLong(secondaryTs);
                this.connection.setSecondaryTs(secondaryTsValue);
                this.connection.getSession().setSnapshot(secondaryTs);
            }
        }else if(this.ticdc.getGlobalSecondaryTs().get() != 0 && this.connection.getSecondaryTs() != this.ticdc.getGlobalSecondaryTs().get()){
            this.connection.getSession().setSnapshot(this.ticdc.getGlobalSecondaryTs().get()+"");
            this.connection.setSecondaryTs(this.ticdc.getGlobalSecondaryTs().get());
        }
        return this;
    }

    public String getSnapshot() throws SQLException{
        String ticdcCFname = getTidbSnapshotParameter(TIDB_TICDC_CF_NAME_KEY,null);
        if(ticdcCFname == null){
            return null;
        }
        try (PreparedStatement ps = this.connection.prepareStatement(QUERY_TIDB_SNAPSHOT_SQL)){
            ps.setString(1,ticdcCFname);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                final String secondaryTs = resultSet.getString("secondary_ts");
                if(secondaryTs != null){
                    return secondaryTs;
                }
            }
        }
        return null;
    }
}
