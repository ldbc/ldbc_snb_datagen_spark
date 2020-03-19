package ldbc.snb.datagen.util;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.generator.tools.PowerDistribution;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Testing the power-law distribution used for comment creation date generation.
 * Produces power_law_cdf.csv which is used in power_law_plots.R
 */
public class TestingPowerLawDist {

    public static void main(String[] args) {

        // alpha = 0.4
        PowerDistribution powerDistribution = new PowerDistribution(0.0, 1.0, DatagenParams.alpha);
        Random random = new Random();
        int N = 1000;
        double[] results = new double[N];
        for (int i = 0; i < N; i++) {
            results[i] = powerDistribution.getPowerDist().inverseF(random.nextDouble());
        }
        BufferedWriter out;
        try {
            out = new BufferedWriter(new FileWriter("power_law_cdf.csv"));
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < N - 1; j++) {
                sb.append(results[j]);
                sb.append(",");
            }
            sb.append(results[N - 1]);
            sb.append("\n");
            out.write(sb.toString());
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
