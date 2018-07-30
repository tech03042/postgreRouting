package kr.co.kdelab.postgre.routing.parser;

import kr.co.kdelab.postgre.routing.parser.internal.TestSet;
import kr.co.kdelab.postgre.routing.parser.internal.TestSetFormat;

import java.io.File;
import java.io.FileNotFoundException;

public class Yago extends TestSet {

    private String[] cache = null;
    private int currentIndex;

    private boolean ignoreFirstTime = false;

    public Yago(File file) throws FileNotFoundException {
        super(file);
    }

    private double getCost() {
        return Math.abs(Math.random() * 99) + 1;
    }

    @Override
    public TestSetFormat readLine() {
        try {
            if (cache == null) {
                do {
                    if (!ignoreFirstTime) {
                        readLineInternal();
                        ignoreFirstTime = true;
                    }

                    String plainData = readLineInternal();
                    if (plainData == null)
                        return null;

                    cache = plainData.split(" ");
                } while (cache.length < 2);
                currentIndex = 1;
            }

            TestSetFormat testSetFormat = new TestSetFormat(Integer.parseInt(cache[0]), Integer.parseInt(cache[currentIndex++]), getCost());
            if (currentIndex >= cache.length)
                cache = null;
            return testSetFormat;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
