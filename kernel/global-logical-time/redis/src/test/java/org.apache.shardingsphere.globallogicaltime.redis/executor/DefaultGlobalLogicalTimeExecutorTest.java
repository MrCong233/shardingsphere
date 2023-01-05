package org.apache.shardingsphere.globallogicaltime.redis.executor;


import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertNull;

public final class DefaultGlobalLogicalTimeExecutorTest {

    @Test
    public void assertBeforeCommit() throws SQLException {
        Collection<Connection> connectionList = new ArrayList<>();
        assertNull(new DefaultGlobalLogicalTimeExecutor().beforeCommit(connectionList));
    }
}