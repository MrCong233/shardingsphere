/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.transaction.xa.glt;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroup;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * glt service implements by redis
 */
@Slf4j
public class RedisGLTService implements GLTService {
    
    RedisConnector redisConnector;
    
    private RedisGLTService() {
        redisConnector = RedisConnector.getInstance();
    }
    
    private static class RedisGLTServiceHolder {
        
        private static final RedisGLTService INSTANCE = new RedisGLTService();
    }
    
    public static GLTService getInstance() {
        return RedisGLTServiceHolder.INSTANCE;
    }
    
    /**
     * try csn lock before transaction starts to  commit, if success get current global csn and send to each dns
     *
     * @param connectionList the connections to each dns
     * @return csn lock
     * @throws SQLException SQL exception
     */
    @Override
    public String gltBeforeCommit(final Collection<Connection> connectionList) throws SQLException {
        // try csn lock before transaction starts to commit
        String csnLockId = RedisConnector.getTryLockId();
        try {
            redisConnector.gltLockCSN(csnLockId);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        
        // set commit csn to each dns
        String order = "SELECT " + redisConnector.gltGetCurrentCSN() + " AS SETCOMMITCSN;";
        for (Connection CSNConnection : connectionList) {
            Statement statement = CSNConnection.createStatement();
            log.debug(order);
            try {
                statement.execute(order);
            } catch (SQLException e) {
                e.printStackTrace();
                log.error("can not send commit csn!");
                throw new SQLException(e.getMessage());
            } finally {
                statement.close();
            }
        }
        return csnLockId;
    }
    
    /**
     * add 1 to the global csn in redis, and release csn lock
     *
     * @param csnLockId csn lock id
     */
    @Override
    public void gltAfterCommit(String csnLockId) {
        redisConnector.gltGetNextCSN();
        redisConnector.gltUnLockCSN(csnLockId);
    }
    
    /**
     * get snapshot csn from redis and save in a ThreadLocal variable when transaction start
     *
     * @throws SQLException SQL exception
     */
    @Override
    public void gltBeginTransaction() {
        long csn = redisConnector.gltGetCurrentCSN();
        GLTCSNThreadLocal.setCSN(csn);
    }
    
    /**
     * send snapshot csn to each dns after cn send "START TRANSACTION" command;
     *
     * @param connection connection to dn
     * @throws SQLException SQL exception
     */
    @Override
    public void gltSendSnapshotCSNAfterStartTransaction(Connection connection) throws SQLException {
        long csn = GLTCSNThreadLocal.getCSN();
        sendSnapshotCSN(connection, csn);
    }
    
    /**
     * send snapshot csn to each dns in Read Commit isolation level
     *
     * @param inputGroups input groups
     * @throws SQLException SQL Exception
     */
    @Override
    public void gltSendSnapshotCSNInReadCommit(Collection<ExecutionGroup<JDBCExecutionUnit>> inputGroups) throws SQLException {
        List<Connection> connections = new ArrayList<>();
        for (ExecutionGroup<JDBCExecutionUnit> entry : inputGroups) {
            List<JDBCExecutionUnit> inputs = entry.getInputs();
            for (JDBCExecutionUnit input : inputs) {
                Connection connection = input.getStorageResource().getConnection();
                connections.add(connection);
            }
        }
        boolean b = connections.get(0).getTransactionIsolation() == Connection.TRANSACTION_READ_COMMITTED;
        if (!connections.isEmpty()) {
            boolean inTransaction = !connections.get(0).getAutoCommit(); // By default, transactions' are read-commit level
            // By default, transactions' isolation are read-commit
            if (inTransaction) {
                long csn = RedisConnector.getInstance().gltGetCurrentCSN();
                for (Connection connection : connections) {
                    sendSnapshotCSN(connection, csn);
                }
            }
        }
    }
    
    private void sendSnapshotCSN(Connection connection, long csn) throws SQLException {
        String order = "SELECT " + csn + " AS SETSNAPSHOTCSN;";
        try (Statement statement = connection.createStatement()) {
            log.debug(order);
            statement.execute(order);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("can not send snapshot csn.");
            throw new SQLException(e);
        }
    }
}
