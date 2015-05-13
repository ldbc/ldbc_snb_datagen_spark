package ldbc.snb.datagen.generator.distribution;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by aprat on 12/05/15.
 */
public abstract class CumulativeBasedDegreeDistribution implements DegreeDistribution{

    public class CumulativeEntry {
        double prob_;
        int value_;
    }

    private ArrayList<CumulativeEntry> cumulativeProbability_;
    private Random random_;

    public void initialize( Configuration conf ) {
        cumulativeProbability_ = cumulativeProbability( conf );
        random_ = new Random();
    }

    public void reset (long seed){
        random_.setSeed(seed);
    }

    public long nextDegree() {
        double prob = random_.nextDouble();
        int index = binarySearch(cumulativeProbability_,prob);
        return cumulativeProbability_.get(index).value_;
    }

    private int binarySearch( ArrayList<CumulativeEntry> cumulative, double prob ) {
        int upperBound = cumulative.size()-1;
        int lowerBound = 0;
        int midPoint = (upperBound + lowerBound)  / 2;
        while (upperBound > (lowerBound+1)){
            if (cumulative.get(midPoint).prob_ > prob ){
                upperBound = midPoint;
            } else{
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound)  / 2;
        }
        return midPoint;
    }

    public abstract ArrayList<CumulativeEntry> cumulativeProbability( Configuration conf );
}
