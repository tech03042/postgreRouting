package kr.co.kdelab.postgre.routing.redfish.algo;

import java.text.DateFormat;
import java.util.Date;

public class DLog {

    private static boolean use = false;

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_BLACK = "\u001B[30m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_WHITE = "\u001B[37m";


    public static void debug(String method, String message) {
        if (use)
            System.out.printf(ANSI_RED + "D:%s:%s  %s" + ANSI_RESET + "\n", method, DateFormat.getDateTimeInstance().format(new Date(System.currentTimeMillis())), message);
    }


    public static void error(String method, String message) {
        if (use)
            System.out.printf(ANSI_RED + "E:%s:%s  %s" + ANSI_RESET + "\n", method, DateFormat.getDateTimeInstance().format(new Date(System.currentTimeMillis())), message);
    }

}
