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

import ldbc.snb.datagen.generator.distribution.DegreeDistribution;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by aprat on 3/03/15.
 */
public abstract class BucketedDistribution extends DegreeDistribution {

    private ArrayList<Bucket> buckets_;
    private ArrayList<Random> randomDegree_;
    private Random randomPercentile_;

    public abstract ArrayList<Bucket> getBuckets(Configuration conf);

    public void initialize(Configuration conf) {
        buckets_ = this.getBuckets(conf);
        randomPercentile_ = new Random(0);
        randomDegree_ = new ArrayList<Random>();
        for (int i = 0; i < buckets_.size(); i++) {
            randomDegree_.add(new Random(0));
        }
    }

    ;

    public void reset(long seed) {
        Random seedRandom = new Random(53223436L + 1234567 * seed);
        for (int i = 0; i < buckets_.size(); i++) {
            randomDegree_.get(i).setSeed(seedRandom.nextLong());
        }
        randomPercentile_.setSeed(seedRandom.nextLong());
    }

    public long nextDegree() {
        int idx = randomPercentile_.nextInt(buckets_.size());
        double minRange = (buckets_.get(idx).min());
        double maxRange = (buckets_.get(idx).max());
        if (maxRange < minRange) {
            maxRange = minRange;
        }
        long ret = randomDegree_.get(idx).nextInt((int) maxRange - (int) minRange + 1) + (int) minRange;
        return ret;
    }

    @Override
    public double mean(long numPersons) {
        return -1.0;
    }
}
