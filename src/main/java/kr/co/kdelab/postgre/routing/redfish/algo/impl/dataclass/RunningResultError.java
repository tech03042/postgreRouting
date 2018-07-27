package kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass;

import java.io.PrintWriter;
import java.io.StringWriter;

public class RunningResultError extends RunningResult {

    private String message;

    public RunningResultError(String message) {
        super(false);

        this.message = message;
    }

    public RunningResultError(Exception e) {
        super(false);

        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            e.printStackTrace(pw);
            message = sw.toString(); // stack trace as a string
        }
    }

    @Override
    public String toString() {
        return message + "\n";
    }
}
