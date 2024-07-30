package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.util.GeneratorConfiguration;

import java.util.*;

public class ZipfDistribution extends DegreeDistribution {

    private org.apache.commons.math3.distribution.ZipfDistribution zipf;
    private double ALPHA = 2.0;
    private Random random = new Random();
    private Map<Integer, Integer> histogram = new HashMap<>();
    private double[] probabilities;
    private Integer[] values;
    private double mean = 0.0;
    private int maxDegree = 1000;
    private int numSamples = 10000;

    public void initialize(GeneratorConfiguration conf) {
        ALPHA = conf.getDouble("ldbc.snb.datagen.generator.distribution.ZipfDistribution.alpha", ALPHA);
        zipf = new org.apache.commons.math3.distribution.ZipfDistribution(maxDegree, ALPHA);
        for (int i = 0; i < numSamples; ++i) {
            int next = zipf.sample();
            Integer currentValue = histogram.put(next, 1);
            if (currentValue != null) {
                histogram.put(next, currentValue + 1);
            }
        }
        int numDifferentValues = histogram.keySet().size();
        probabilities = new double[numDifferentValues];
        values = new Integer[numDifferentValues];
        histogram.keySet().toArray(values);
        Arrays.sort(values, Comparator.comparingInt(o -> o));

        probabilities[0] = histogram.get(values[0]) / (double) numSamples;
        for (int i = 1; i < numDifferentValues; ++i) {
            int occurrences = histogram.get(values[i]);
            double prob = occurrences / (double) numSamples;
            mean += prob * values[i];
            probabilities[i] = probabilities[i - 1] + prob;
        }
    }

    public void reset(long seed) {
        zipf.reseedRandomGenerator(seed);
        random.setSeed(seed);
    }

    public long nextDegree() {
        int min = 0;
        int max = probabilities.length;
        double prob = random.nextDouble();
        int currentPosition = (max - min) / 2 + min;
        while (max > (min + 1)) {
            if (probabilities[currentPosition] > prob) {
                max = currentPosition;
            } else {
                min = currentPosition;
            }
            currentPosition = (max - min) / 2 + min;
        }
        return values[currentPosition];
    }

    public double mean(long numPersons) {
        return mean;
    }
}
