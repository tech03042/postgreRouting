package kr.co.kdelab.postgre.routing.redfish.reachability;

import kr.co.kdelab.postgre.routing.redfish.reachability.dataclass.RechabilityResult;
import kr.co.kdelab.postgre.routing.redfish.reachability.util.PreparedStatementArray;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class RechabilityCalculator implements Closeable {
    private Connection connection;
    private Statement statement;
    private PreparedStatementArray preparedStatements = new PreparedStatementArray();
    // PreparedStatement -> Array -> Close


    public RechabilityCalculator(Connection connection) throws SQLException {
        this.connection = connection;
        statement = connection.createStatement();
    }

    public Connection getConnection() {
        return connection;
    }

    protected PreparedStatement newPreparedStatement(PreparedStatement preparedStatement) {
        preparedStatements.add(preparedStatement);
        return preparedStatement;
    }

    public RechabilityResult calc(int source, int target) throws Exception {
        return null;
    }

    protected Statement getCloseStatement() {
        return statement;
    }

    @Override
    public void close() throws IOException {
        preparedStatements.closeAll();
        try {
            connection.close();
        } catch (SQLException ignored) {

        }
    }
}
