package kr.co.kdelab.postgre.routing.parser;

import kr.co.kdelab.postgre.routing.parser.internal.TestSet;
import kr.co.kdelab.postgre.routing.parser.internal.TestSetFormat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Random extends TestSet {
    public Random(File file) throws FileNotFoundException {
        super(file);
    }

    public Random() throws FileNotFoundException {
        super(new File("dataSet/random_5000_50000_10.txt"));
    }

    @Override
    public TestSetFormat readLine() {
        try {
            String plainData = readLineInternal();
            if (plainData == null)
                return null;

            String[] dataSplited = plainData.split(" |,");
            return new TestSetFormat(Integer.parseInt(dataSplited[0]), Integer.parseInt(dataSplited[1]), Math.abs(Double.parseDouble(dataSplited[2])));
        } catch (IOException e) {
//            Log.w(e.getMessage());
        }
        return null;
    }
}
