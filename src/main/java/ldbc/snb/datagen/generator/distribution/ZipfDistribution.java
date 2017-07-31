/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.generator.distribution;

import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by aprat on 5/03/15.
 */
public class ZipfDistribution extends DegreeDistribution {

    private org.apache.commons.math3.distribution.ZipfDistribution zipf_;
    private double ALPHA_ = 2.0;
    private Random random = new Random();
    private HashMap<Integer, Integer> histogram = new HashMap<Integer, Integer>();
    private double probabilities[];
    private Integer values[];
    private double mean_ = 0.0;
    private int maxDegree = 1000;
    private int numSamples = 10000;

    public void initialize(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.ZipfDistribution.alpha", ALPHA_);
        zipf_ = new org.apache.commons.math3.distribution.ZipfDistribution(maxDegree, ALPHA_);
        for (int i = 0; i < numSamples; ++i) {
            int next = zipf_.sample();
            Integer currentValue = histogram.put(next, 1);
            if (currentValue != null) {
                histogram.put(next, currentValue + 1);
            }
        }
        int numDifferentValues = histogram.keySet().size();
        probabilities = new double[numDifferentValues];
        values = new Integer[numDifferentValues];
        histogram.keySet().toArray(values);
        Arrays.sort(values, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });

        probabilities[0] = histogram.get(values[0]) / (double) numSamples;
        for (int i = 1; i < numDifferentValues; ++i) {
            int occurrences = histogram.get(values[i]);
            double prob = occurrences / (double) numSamples;
            mean_ += prob * values[i];
            probabilities[i] = probabilities[i - 1] + prob;
        }
    }

    public void reset(long seed) {
        zipf_.reseedRandomGenerator(seed);
        random.setSeed(seed);
    }

    public long nextDegree() {
        //return zipf_.sample();
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
        //return zipf_.getNumericalMean();
        return mean_;
    }
}
