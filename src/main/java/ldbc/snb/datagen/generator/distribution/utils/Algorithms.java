package ldbc.snb.datagen.generator.distribution.utils;

import ldbc.snb.datagen.generator.distribution.DegreeDistribution;

/**
 * Created by aprat on 3/02/17.
 */
public class Algorithms {
    private static double scale(long numPersons, double mean) {
        return Math.log10(mean*numPersons/2+numPersons);
    }

    public static long findNumPersonsFromGraphalyticsScale(DegreeDistribution distribution, double scale) {

        long numPersonsMin = 1000000;
        while(scale(numPersonsMin,distribution.mean(numPersonsMin)) > scale) {
            numPersonsMin /= 2;
        }

        long numPersonsMax = 1000000;
        while(scale(numPersonsMax,distribution.mean(numPersonsMax)) < scale) {
            numPersonsMax *= 2;
        }

        long currentNumPersons = (numPersonsMax - numPersonsMin) / 2 + numPersonsMin;
        double currentScale = scale(currentNumPersons, distribution.mean(currentNumPersons));
        while(Math.abs(currentScale - scale) / scale > 0.001) {
            if(currentScale < scale ) {
                numPersonsMin = currentNumPersons;
            } else {
                numPersonsMax = currentNumPersons;
            }
            currentNumPersons = (numPersonsMax - numPersonsMin) / 2 + numPersonsMin;
            currentScale = scale(currentNumPersons, distribution.mean(currentNumPersons));
            //System.out.println(numPersonsMin+" "+numPersonsMax+" "+currentNumPersons+" "+currentScale);
        }
        return currentNumPersons;
    }
}
