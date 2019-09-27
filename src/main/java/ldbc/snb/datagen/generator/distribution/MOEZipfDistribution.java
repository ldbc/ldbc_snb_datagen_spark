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

import java.util.Random;

/**
 * Created by aprat on 4/03/15.
 */
public class MOEZipfDistribution extends DegreeDistribution {

    private org.apache.commons.math3.distribution.ZipfDistribution zipf_;
    private double ALPHA_ = 1.7;
    private double DELTA_ = 1.5;
    private Random random_;

    public void initialize(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.MOEZipfDistribution.alpha", ALPHA_);
        DELTA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.MOEZipfDistribution.delta", DELTA_);
        zipf_ = new org.apache.commons.math3.distribution.ZipfDistribution(5000, ALPHA_);
        random_ = new Random();
    }

    public void reset(long seed) {
        random_.setSeed(seed);
        zipf_.reseedRandomGenerator(seed);
    }

    public long nextDegree() {
        double prob = random_.nextDouble();
        double prime = (prob * DELTA_) / (1 + prob * (DELTA_ - 1));
        long ret = zipf_.inverseCumulativeProbability(prime);
        return ret;
    }

}

