package kr.co.kdelab.postgre.routing.parser.applier;

import kr.co.kdelab.postgre.routing.parser.internal.TestSet;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class TableApplier {
    TestSet testSetReader = null;

    public TableApplier() {

    }

    public TableApplier(TestSet testSet) {
        this.testSetReader = testSet;
    }

    public void setReader(TestSet testSetReader) {
        this.testSetReader = testSetReader;
    }

    public abstract void applyInTable(Connection connection) throws SQLException;
}
