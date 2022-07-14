package com.tidb.jdbc;

import com.mysql.cj.jdbc.ConnectionImpl;
import com.tidb.snapshot.Ticdc;

public class TidbCdcWrapper {

    public ConnectionImpl connection;

    public Ticdc ticdc;

    public void refreshSnapshot(){
        try {
            TidbCdcOperate.of(connection,ticdc).refreshSnapshot();
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
}
