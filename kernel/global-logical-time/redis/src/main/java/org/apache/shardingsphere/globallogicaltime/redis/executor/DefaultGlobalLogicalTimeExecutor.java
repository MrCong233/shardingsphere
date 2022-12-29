package org.apache.shardingsphere.globallogicaltime.redis.executor;

import org.apache.shardingsphere.globallogicaltime.spi.GlobalLogicalTimeExecutor;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroup;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc.JDBCExecutionUnit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

/**
 * Default global logical time executor, which is turned off by default
 */
public class DefaultGlobalLogicalTimeExecutor implements GlobalLogicalTimeExecutor {

    @Override
    public String beforeCommit(Collection<Connection> connectionList) throws SQLException {
        return null;
    }

    @Override
    public void afterCommit(String csnLockId) {

    }

    @Override
    public void getSnapshotCSNWhenBeginTransaction() throws SQLException {

    }

    @Override
    public void sendSnapshotCSNAfterStartTransaction(Connection connection) throws SQLException {

    }

    @Override
    public void sendSnapshotCSNInReadCommit(Collection<ExecutionGroup<JDBCExecutionUnit>> inputGroups) throws SQLException {

    }
}
