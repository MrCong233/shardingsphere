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
 * glt off service
 */
public class GLTOffService implements GLTService {
    
    private GLTOffService() {
        
    }
    
    private static class GLTOffServiceHolder {
        
        private static final GLTOffService INSTANCE = new GLTOffService();
    }
    
    /**
     * get instance
     *
     * @return  singleton instance of GLTOffService
     */
    public static GLTOffService getInstance() {
        return GLTOffServiceHolder.INSTANCE;
    }
    
    /**
     * before xa transaction start to commit
     * return null when glt off
     *
     * @param connectionList
     * @return csnLockId
     */
    @Override
    public String gltBeforeCommit(final Collection<Connection> connectionList) {
        return null;
    }
    
    /**
     * xa transaction commit completed
     * do nothing when glt off
     *
     * @param csnLockId
     */
    @Override
    public void gltAfterCommit(String csnLockId) {
        
    }
    
    /**
     * start transaction
     * do nothing when glt off
     *
     */
    @Override
    public void gltBeginTransaction() {
        
    }
    
    /**
     * send snapshot csn to a dn after cn send "start transaction" to the dn
     * do nothing when glt off
     *
     * @param connection
     */
    @Override
    public void gltSendSnapshotCSNAfterStartTransaction(Connection connection) {
        
    }
    
    /**
     * send snapshot csn to each dns in Read Commit isolation level.
     * do nothing when glt off.
     *
     * @param inputGroups
     * @throws SQLException
     */
    @Override
    public void gltSendSnapshotCSNInReadCommit(Collection<ExecutionGroup<JDBCExecutionUnit>> inputGroups) throws SQLException {
        
    }
}
