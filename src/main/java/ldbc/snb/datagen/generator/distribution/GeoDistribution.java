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

import org.apache.commons.math3.distribution.GeometricDistribution;
import org.apache.hadoop.conf.Configuration;


/**
 * Created by aprat on 5/03/15.
 */
public class GeoDistribution extends DegreeDistribution {

    private GeometricDistribution geo_;
    private double ALPHA_ = 0.12;

    public void initialize(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.GeoDistribution.alpha", ALPHA_);
        geo_ = new GeometricDistribution(ALPHA_);
    }

    public void reset(long seed) {
        geo_.reseedRandomGenerator(seed);
    }

    public long nextDegree() {
        return geo_.sample();
    }

    @Override
    public double mean(long numPersons) {
        return geo_.getNumericalMean();
    }
}
