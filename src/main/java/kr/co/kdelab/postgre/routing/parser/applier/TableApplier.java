package kr.co.kdelab.postgre.routing.parser.applier;

import kr.co.kdelab.postgre.routing.parser.internal.TestSet;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class TableApplier {
    private TestSet testSetReader = null;

    TableApplier() {

    }

    TableApplier(TestSet testSet) {
        this.testSetReader = testSet;
    }

    public void setReader(TestSet testSetReader) {
        this.testSetReader = testSetReader;
    }

    public TestSet getReader() {
        return testSetReader;
    }

    public abstract void applyInTable(Connection connection) throws SQLException;
}
