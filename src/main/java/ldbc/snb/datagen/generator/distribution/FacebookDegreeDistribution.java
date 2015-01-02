/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package ldbc.snb.datagen.generator.distribution;

import java.io.*;
import java.util.Random;
import ldbc.snb.datagen.generator.DatagenParams;

/**
 * This class generates Facebook-like social degree distribution
 * <p/>
 * A. Preparation
 * For a specific social network size (total number of users)
 * 1) compute the mean value of social degree
 * 2) compute the range of each bucket (100 buckets) using the data from facebookBucket100.dat
 * B. Generate social degree for each user
 * 1) Determine the bucket (Take a random number from 0-99)
 * 2) Randomly select a social degree in the range of that bucket
 */

public class FacebookDegreeDistribution implements DegreeDistribution {
    private int mean_ = 0;
    private Random randomPercentile_;
    private Random randomDegree_[];
    private Bucket[] buckets_;
    private static final int FB_MEAN_ = 190;
    private static final int BUCKET_NUM_ = 100;
    private int[] counter_;  		/* Count the number of users at specific social degree */
    private int[] percenttileIDCounter_;	/* Current user Id in a certain percentile */
    private int percentileIdx_; 			/* Store the Idx of the current percentile */

    private class Bucket {
        double min;
        double max;
        int minRange;
        int maxRange;
        int percentile;

        public Bucket(double min, double max, int percentile) {
            this.min = min;
            this.max = max;
            this.percentile = percentile;
        }

        public double getMin() {
            return min;
        }

        public void setMin(double min) {
            this.min = min;
        }

        public double getMax() {
            return max;
        }

        public void setMax(double max) {
            this.max = max;
        }

        public int getPercentile() {
            return percentile;
        }

        public void setPercentile(int percentile) {
            this.percentile = percentile;
        }

        public int getMinRange() {
            return minRange;
        }

        public void setMinRange(int minRange) {
            this.minRange = minRange;
        }

        public int getMaxRange() {
            return maxRange;
        }

        public void setMaxRange(int maxRange) {
            this.maxRange = maxRange;
        }

    }

    public FacebookDegreeDistribution() {
    }

    public void initialize() {
        mean_ = (int) Math.round(Math.pow(DatagenParams.numPersons, (0.512 - 0.028 * Math.log10(DatagenParams.numPersons))));
        System.out.println("Mean = " + mean_);
        randomPercentile_ = new Random(0);
        percenttileIDCounter_ = new int[BUCKET_NUM_];
        percentileIdx_ = -1;
        randomDegree_ = new Random[BUCKET_NUM_];
        for (int i = 0; i < BUCKET_NUM_; i++) {
            randomDegree_[i] = new Random(0);
            percenttileIDCounter_[i] = 0;
        }
        buckets_ = new Bucket[BUCKET_NUM_];
        loadFBBuckets();
        rebuildBucketRange();
    }

    public void reset(long seed) {
        Random seedRandom = new Random(53223436L + 1234567 * seed);
        for (int i = 0; i < BUCKET_NUM_; i++) {
            randomDegree_[i].setSeed(seedRandom.nextLong());
        }
        randomPercentile_.setSeed(seedRandom.nextLong());
    }

    public void loadFBBuckets() {
        try {
            BufferedReader fbDataReader = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(DatagenParams.fbSocialDegreeFile), "UTF-8"));
            String line;
            int idx = 0;
            int percentile = 0;
            while ((line = fbDataReader.readLine()) != null) {
                percentile++;
                String data[] = line.split(" ");
                buckets_[idx] = new Bucket(Double.parseDouble(data[0]), Double.parseDouble(data[1]), percentile);
                idx++;
            }
            fbDataReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void rebuildBucketRange() {
        double newMin, newMax;
        int minRange, maxRange;
        for (int i = 0; i < BUCKET_NUM_; i++) {
            newMin = buckets_[i].getMin() * mean_ / FB_MEAN_;
            newMax = buckets_[i].getMax() * mean_ / FB_MEAN_;

            buckets_[i].setMin(newMin);
            buckets_[i].setMax(newMax);

            //set the range
            minRange = (int) Math.floor(buckets_[i].getMin() + 1);
            maxRange = (int) Math.floor(buckets_[i].getMax());
            if (maxRange < minRange) maxRange = minRange;

            buckets_[i].setMinRange(minRange);
            buckets_[i].setMaxRange(maxRange);
        }

        //Init counter
        int maxCounterIdx = buckets_[BUCKET_NUM_ - 1].getMaxRange();
        counter_ = new int[maxCounterIdx + 1];
        for (int i = 0; i < (maxCounterIdx + 1); i++) {
            counter_[i] = 0;
        }

    }

    public long nextDegree() {
        int degree;
        int idx = randomPercentile_.nextInt(100);
        percentileIdx_ = idx;
        degree = randomDegree_[idx].nextInt(buckets_[idx].getMaxRange() - buckets_[idx].getMinRange() + 1) + buckets_[idx].getMinRange();
        counter_[degree]++;
        return degree;
    }
}
