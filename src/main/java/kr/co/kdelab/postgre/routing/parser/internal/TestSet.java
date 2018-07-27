package kr.co.kdelab.postgre.routing.parser.internal;

import kr.co.kdelab.postgre.routing.parser.applier.DefaultApplier;
import kr.co.kdelab.postgre.routing.parser.applier.TableApplier;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

public abstract class TestSet implements AutoCloseable {
    private BufferedReader fileBuffer;
    private TableApplier applier = new DefaultApplier(this);

    protected TestSet(File file) throws FileNotFoundException {
        fileBuffer = new BufferedReader(new FileReader(file));
    }


    protected String readLineInternal() throws IOException {
        return fileBuffer.readLine();
    }

    public abstract TestSetFormat readLine();

    public void setApplier(TableApplier tableApplier) {
        this.applier = tableApplier;
        this.applier.setReader(this);
    }

    public void applyInTable(Connection connection) throws SQLException {
        applier.applyInTable(connection);
    }

    @Override
    public void close() throws IOException {
        fileBuffer.close();
    }
}
