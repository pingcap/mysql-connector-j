/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.tidb.jdbc;

import com.mysql.cj.jdbc.ConnectionImpl;
import com.tidb.snapshot.Ticdc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class TidbCdcOperate {

    private static final String QUERY_TIDB_SNAPSHOT_SQL =
            "select `secondary_ts` from `tidb_cdc`.`syncpoint_v1` where `cf` = ? order by `primary_ts` desc limit 1";


    private static final String QUERY_CFNAME_SQL = "select cf from `tidb_cdc`.`syncpoint_v1` group by cf";

    public ConnectionImpl connection;

    public Ticdc ticdc;

    private AtomicReference<PreparedStatement> preparedStatement;

    private Boolean closeFlag = true;

    public TidbCdcOperate(ConnectionImpl connection,Ticdc ticdc){
        this.connection = connection;
        this.ticdc = ticdc;
    }

    public static TidbCdcOperate of(ConnectionImpl connection,Ticdc ticdc){
        return new TidbCdcOperate(connection,ticdc);
    }



    private Boolean isRun(){
        String useTicdcACID = this.ticdc.getUseTicdcACID();
        if(useTicdcACID == null){
            return false;
        }
        if(!"true".equals(useTicdcACID)){
            return false;
        }
        return true;
    }

    /**
     *
     * proxy refresh connection Snapshot
     *
     * @return
     */
    public TidbCdcOperate refreshSnapshot() throws Exception{
        if(!isRun()){
            return this;
        }
        try {
            if(this.connection != null){
                setSnapshot();
            }
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
        return this;
    }

    public TidbCdcOperate setPreparedStatement(AtomicReference<PreparedStatement> preparedStatement){
        this.preparedStatement = preparedStatement;
        this.closeFlag = false;
        return this;
    }

    /**
     *
     * set Connection Snapshot
     * @param secondaryTs
     */
    private void setConnectionSnapshot(Long secondaryTs){
        this.connection.getSession().setSnapshot(secondaryTs+"");
        this.connection.setSecondaryTs(secondaryTs);
    }

    /**
     *
     * set connection SecondaryTs and Session() SecondaryTs by GlobalSecondaryTs
     * if GlobalSecondaryTs is null get SecondaryTs by db
     * @return
     * @throws SQLException
     */
    public TidbCdcOperate setSnapshot() throws SQLException{
        if(this.connection.getSecondaryTs() == 0){
            String secondaryTs = getSnapshot();
            if(secondaryTs != null){
                Long secondaryTsValue = Long.parseLong(secondaryTs);
                setConnectionSnapshot(secondaryTsValue);
            }
            return this;
       }
        if(this.ticdc.getGlobalSecondaryTs().get() == 0){
            return this;
        }
        if(this.connection.getSecondaryTs() == this.ticdc.getGlobalSecondaryTs().get()){
            return this;
        }
        setConnectionSnapshot(this.ticdc.getGlobalSecondaryTs().get());
        return this;
    }

    /**
     *
     * monitor reuse use preparedStatement
     * use conn create preparedStatement
     * @return getSnapshot
     * @throws SQLException
     */
    public String getSnapshot() throws SQLException{
        ResultSet resultSet = null;
        try {
            getCFname();
            if(this.preparedStatement == null){
                this.preparedStatement = new AtomicReference<>();
            }
            if(this.preparedStatement.get() == null){
                this.preparedStatement.set(this.connection.prepareStatement(QUERY_TIDB_SNAPSHOT_SQL));
            }
            this.preparedStatement.get().setString(1,this.ticdc.getTicdcCFname());
            resultSet = this.preparedStatement.get().executeQuery();
            while (resultSet.next()) {
                final String secondaryTs = resultSet.getString("secondary_ts");
                if(secondaryTs != null){
                    return secondaryTs;
                }
            }
            throw new SQLException("Table "+this.ticdc.getTicdcCFname()+" Record secondary_ts is empty");
        } catch (SQLException e){
            throw new SQLException(e);
        } finally {
            if(closeFlag){
                if(preparedStatement != null){
                    this.preparedStatement.get().close();
                }
            }
            if(resultSet != null){
                resultSet.close();
            }
        }
    }

    /**
     *
     * monitor getCFname
     * @return getSnapshot
     * @throws SQLException
     */
    public void getCFname() throws SQLException{
        if(this.ticdc.getTicdcCFname() != null){
            return;
        }
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = this.connection.prepareStatement(QUERY_CFNAME_SQL);
            resultSet = preparedStatement.executeQuery();
            String cf = null;
            while (resultSet.next()) {
                if(cf == null){
                    cf = resultSet.getString("cf");
                }else {
                    throw new SQLException("Found multiple changefeeds, recommend to set ticdcCFname to specify changefeed");
                }
            }
            if(cf == null){
                throw new SQLException("not Found changefeeds task name");
            }
            //System.out.println("ticdc-cfname:"+cf);
            this.ticdc.setTicdcCFname(cf);
        } catch (SQLException e){
            throw new SQLException(e);
        } finally {
            if(preparedStatement != null){
                preparedStatement.close();
            }
            if(resultSet != null){
                resultSet.close();
            }
        }
    }
}
