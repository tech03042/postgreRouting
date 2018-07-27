package kr.co.kdelab.postgre.routing.parser;

import kr.co.kdelab.postgre.routing.parser.internal.TestSet;
import kr.co.kdelab.postgre.routing.parser.internal.TestSetFormat;

import java.io.File;
import java.io.FileNotFoundException;

public class USARoad extends TestSet {
    public USARoad(File file) throws FileNotFoundException {
        super(file);
    }

    public USARoad() throws FileNotFoundException {
        super(new File("dataSet/USA-road-t.NY.gr"));
    }

    @Override
    public TestSetFormat readLine() {
        try {
            String string;

            do {
                string = readLineInternal();
                if (string == null)
                    return null;
            } while (!string.startsWith("a"));

            String[] splitted = string.split(" ");
            return new TestSetFormat(Integer.parseInt(splitted[1]), Integer.parseInt(splitted[2]), Double.parseDouble(splitted[3]));
        } catch (Exception e) {
            return null;
        }
    }
}
