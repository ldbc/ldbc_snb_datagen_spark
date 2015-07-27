package ldbc.snb.datagen.generator.tools;

import ldbc.snb.datagen.objects.Knows;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

/**
 * Created by aprat on 17/06/15.
 */
public class MinHash<T> {

    private int numHashes_ = 0;
    private int a[];
    private int b[];
    private int p[];
    private Random random_;

    public MinHash( int numHashes, int seed ) {
        numHashes_ =  numHashes;
        random_ =new Random();
        a = new int[numHashes];
        b = new int[numHashes];
        p = new int[numHashes];
        for(int i = 0; i < numHashes; ++i ) {
            a[i] = random_.nextInt();
            b[i] = random_.nextInt();
            p[i] = random_.nextInt();
        }
    }

    public ArrayList<Long> minHash( Set<Long> set ) {
        ArrayList<Long> minHashes = new ArrayList<Long>();
        for(int i = 0; i < numHashes_; ++i ) {
            long min = Long.MAX_VALUE;
            long minl = 0;
            for( Long l : set ) {
                long hash = (a[i]*l+b[i]) % p[i];
                if(hash <= min) {
                    min = hash;
                    minl = l;

                }
            }
            minHashes.add(minl);
        }
        return minHashes;
    }
}
