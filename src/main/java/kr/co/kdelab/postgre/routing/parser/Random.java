package kr.co.kdelab.postgre.routing.parser;

import kr.co.kdelab.postgre.routing.parser.internal.TestSet;
import kr.co.kdelab.postgre.routing.parser.internal.TestSetFormat;

import java.io.File;
import java.io.FileNotFoundException;

public class Random extends TestSet {

    private WeightPolicy weightPolicy = WeightPolicy.DEFAULT;

    public enum WeightPolicy {
        DEFAULT,
        RANDOM_WEIGHT,
        WEIGHT_FIX_1
    }

    public Random(File file) throws FileNotFoundException {
        super(file);
    }

    public void setWeightPolicy(WeightPolicy weightPolicy) {
        this.weightPolicy = weightPolicy;
    }

    public WeightPolicy getWeightPolicy() {
        return weightPolicy;
    }

    private double getRandomWeight() {
        return Math.abs(Math.random() * 99) + 1;
    }

    @Override
    public TestSetFormat readLine() {
        String plainData = null;
        try {
            do {
                plainData = readLineInternal();
                if (plainData == null)
                    return null;
            } while (plainData.startsWith("#"));
            String[] dataSplited = plainData.split("[ ,\t]");
            int source, target;
            double weight;
            source = Integer.parseInt(dataSplited[0]);
            target = Integer.parseInt(dataSplited[1]);

            if (dataSplited.length < 3) {
                switch (weightPolicy) {
                    case WEIGHT_FIX_1:
                        weight = 1.0f;
                        break;
                    case RANDOM_WEIGHT:
                        weight = getRandomWeight();
                        break;
                    default:
                        weight = getRandomWeight();
                        break;
                }
            } else
                weight = Integer.parseInt(dataSplited[2]);
            return new TestSetFormat(source, target, weight);
        } catch (Exception ignored) {
            if (plainData == null)
                plainData = "NULL";
            System.out.printf("PARSING ERROR (%s)\n", plainData);
        }
        return null;
    }
}
