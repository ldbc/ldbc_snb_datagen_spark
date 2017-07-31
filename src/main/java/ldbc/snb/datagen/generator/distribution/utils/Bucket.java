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
package ldbc.snb.datagen.generator.distribution.utils;

import ldbc.snb.datagen.generator.DatagenParams;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;

/**
 * Created by aprat on 3/03/15.
 */
public class Bucket {

    private double min_;
    private double max_;

    public static ArrayList<Bucket> bucketizeHistogram(ArrayList<Pair<Integer, Integer>> histogram, int num_buckets) {

        ArrayList<Bucket> buckets = new ArrayList<Bucket>();
        int population = 0;
        int num_edges = 0;
        for (Pair<Integer, Integer> i : histogram) {
            population += i.getValue();
            num_edges += i.getValue() * i.getKey();
        }
        num_edges /= 2;


        int avgDegreeAt1B = 200;
        int avgDegree = num_edges / population;
        double aCoeff = Math.log(avgDegreeAt1B) / Math.log(1000000000);
        double bCoeff = (aCoeff - (Math.log(avgDegree) / Math.log(population))) / Math.log10(population);

        int target_mean = (int) Math.round(Math.pow(DatagenParams.numPersons, (aCoeff - bCoeff * Math
                .log10(DatagenParams.numPersons))));
        System.out.println("Distribution mean degree: " + avgDegree + " Distribution target mean " + target_mean);
        int bucket_size = (int) (Math.ceil(population / (double) (num_buckets)));
        int current_histogram_index = 0;
        int current_histogram_left = histogram.get(current_histogram_index).getValue();
        for (int i = 0; i < num_buckets && (current_histogram_index < histogram.size()); ++i) {
            int current_bucket_count = 0;
            int min = population;
            int max = 0;
            while (current_bucket_count < bucket_size && current_histogram_index < histogram.size()) {
                int degree = histogram.get(current_histogram_index).getKey();
                min = degree < min ? degree : min;
                max = degree > max ? degree : max;
                if ((bucket_size - current_bucket_count) > current_histogram_left) {
                    current_bucket_count += current_histogram_left;
                    current_histogram_index++;
                    if (current_histogram_index < histogram.size()) {
                        current_histogram_left = histogram.get(current_histogram_index).getValue();
                    }
                } else {
                    current_histogram_left -= (bucket_size - current_bucket_count);
                    current_bucket_count = bucket_size;
                }
            }
            min = (int) (min * target_mean / (double) avgDegree);
            max = (int) (max * target_mean / (double) avgDegree);
            buckets.add(new Bucket(min, max));
        }
        return buckets;
    }


    public Bucket(double min, double max) {
        this.min_ = min;
        this.max_ = max;
    }

    public double min() {
        return min_;
    }

    public void min(double min) {
        min_ = min;
    }

    public double max() {
        return max_;
    }

    public void max(double max) {
        max_ = max;
    }
}
