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

package org.apache.shardingsphere.infra.metadata.database.schema.ddl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GLTDDLHandler {
    
    private static boolean enableGLT;
    /* if the thread is in a transaction */
    private static ThreadLocal<Boolean> inTransaction;
    
    /* if the last sql statement is ddl */
    private static ThreadLocal<Boolean> isDDL;
    
    private static ThreadLocal<Map<String, Connection>> connections;
    
    static {
        enableGLT = false;
        inTransaction = new ThreadLocal<>();
        isDDL = new ThreadLocal<>();
        connections = new ThreadLocal<>();
    }
    
    public static void setGLTConfig(boolean flag) {
        if (flag) {
            enableGLT = true;
        }
    }
    
    public static boolean isEnableGLT() {
        return enableGLT;
    }
    public static void startTransaction() {
        inTransaction.set(true);
        isDDL.set(false);
        if (connections.get() != null) {
            connections.get().clear();
        } else {
            connections.set(new HashMap<>());
        }
    }
    
    public static void endTransaction() {
        inTransaction.set(false);
        isDDL.set(false);
        if (connections.get() != null) {
            connections.get().clear();
        } else {
            connections.set(new HashMap<>());
        }
    }
    
    public static boolean isInTransaction() {
        if (inTransaction.get() == null || inTransaction.get().equals(false)) {
            return false;
        }
        return true;
    }
    
    public static boolean setConnection(String dataSourceName, Connection connection) throws SQLException {
        if (connection == null || connection.isClosed()) {
            return false;
        }
        Map<String, Connection> map = connections.get();
        map.put(dataSourceName, connection);
        return true;
    }
    public static Connection getConnection(String dataSourceName) throws SQLException {
        return connections.get().getOrDefault(dataSourceName, null);
    }
    
    public static List<Connection> getConnection(List<String> dataSourceNames) throws SQLException {
        List<Connection> result = new ArrayList<>();
        for (String dataSourceName : dataSourceNames) {
            result.add(getConnection(dataSourceName));
        }
        return result;
    }
}
