package ldbc.snb.datagen.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PersonDeleteDistribution {

    private double[] distribution;
    private String distributionFile;

    public PersonDeleteDistribution(String distributionFile) {
        this.distributionFile = distributionFile;
    }

    public void initialize() {
        try {
            BufferedReader distributionBuffer = new BufferedReader(new InputStreamReader(getClass()
                    .getResourceAsStream(distributionFile), StandardCharsets.UTF_8));
            List<Double> temp = new ArrayList<>();
            String line;
            while ((line = distributionBuffer.readLine()) != null) {
                Double prob = Double.valueOf(line);
                temp.add(prob);
            }
            distribution = new double[temp.size()];
            int index = 0;
            for (Double aDouble : temp) {
                distribution[index] = aDouble;
                ++index;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean isDeleted(Random random, long maxKnows) {
        return random.nextDouble() < distribution[(int) maxKnows];

    }


}
