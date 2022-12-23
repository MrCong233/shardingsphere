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

import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroup;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

/**
 * glt service
 */
public interface GLTService {
    
    /**
     * before xa transaction start to commit
     *
     * @param connectionList
     * @return csnLockId
     */
    String gltBeforeCommit(final Collection<Connection> connectionList) throws SQLException;
    
    /**
     * xa transaction commit completed
     *
     * @param csnLockId
     */
    void gltAfterCommit(String csnLockId);
    
    /**
     * start transaction
     */
    void gltBeginTransaction() throws SQLException;
    
    /**
     * send snapshot csn to a dn after cn send "start transaction" to the dn
     *
     * @param connection
     */
    void gltSendSnapshotCSNAfterStartTransaction(Connection connection) throws SQLException;
    
    /**
     * send snapshot csn to each dns in Read Commit isolation level.
     *
     * @param inputGroups
     * @throws SQLException
     */
    void gltSendSnapshotCSNInReadCommit(Collection<ExecutionGroup<JDBCExecutionUnit>> inputGroups) throws SQLException;
}
