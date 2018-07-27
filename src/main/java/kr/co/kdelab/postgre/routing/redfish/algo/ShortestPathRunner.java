package kr.co.kdelab.postgre.routing.redfish.algo;

import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.util.PreparedStatementArray;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class ShortestPathRunner implements Closeable {
    private int source;
    private int target;

    private JDBConnectionInfo jdbConnectionInfo;
    private Connection connection;
    private PreparedStatementArray preparedStatements = new PreparedStatementArray();

    public RunningResult run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        this.jdbConnectionInfo = jdbConnectionInfo;
        connection = jdbConnectionInfo.createConnection();

        return run();
    }

    public JDBConnectionInfo getJdbConnectionInfo() {
        return jdbConnectionInfo;
    }


    public Connection getConnection() {
        return connection;
    }

    protected PreparedStatement addPreparedStatement(PreparedStatement preparedStatement) {
        preparedStatements.add(preparedStatement);
        return preparedStatement;
    }

    @Override
    public void close() throws IOException {
        preparedStatements.closeAll();

        try {
            connection.close();
        } catch (SQLException ignored) {
        }
    }

    public abstract RunningResult run() throws Exception;

    public int getSource() {
        return source;
    }

    public void setSource(int source) {
        this.source = source;
    }

    public int getTarget() {
        return target;
    }

    public void setTarget(int target) {
        this.target = target;
    }
}
