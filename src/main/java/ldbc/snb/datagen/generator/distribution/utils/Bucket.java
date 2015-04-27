package ldbc.snb.datagen.generator.distribution.utils;

import java.util.ArrayList;

/**
 * Created by aprat on 3/03/15.
 */
public class Bucket {

    public static ArrayList<Bucket> bucketizeHistogram(ArrayList<Double> histogram, int num_buckets) {

        ArrayList<Bucket> buckets = new ArrayList<Bucket>();
        Double population = 0.0;
        for( Double d : histogram )  {
           population+=d;
        }
        int percentile_size = population.intValue() / num_buckets;
        int current_histogram_index = 0;
        int current_histogram_counter = histogram.get(current_histogram_index).intValue();
        for( int i = 0; i < num_buckets; ++i ) {
            double min = population.intValue();
            double max = 0;
            for( int j = 0; j < percentile_size; ++j ) {
                min = min > (current_histogram_index+1) ? (current_histogram_index+1) : min;
                max = max < (current_histogram_index+1) ? (current_histogram_index+1) : max;
                if(--current_histogram_counter <= 0) {
                    current_histogram_index++;
                    current_histogram_counter = histogram.get(current_histogram_index).intValue();
                }
            }
            buckets.add(new Bucket(min, max));
        }
        return buckets;
    }

    double min_;
    double max_;

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
