package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.util.GeneratorConfiguration;

public abstract class DegreeDistribution {

    public abstract void initialize(GeneratorConfiguration conf);

    public abstract void reset(long seed);

    public abstract long nextDegree();

    public double mean(long numPersons) {
        return -1;
    }
}
