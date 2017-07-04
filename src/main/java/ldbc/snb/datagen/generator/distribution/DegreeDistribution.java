/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ldbc.snb.datagen.generator.distribution;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author aprat
 */
public abstract class DegreeDistribution {

	public abstract void initialize( Configuration conf );

    public abstract void reset (long seed);

	public abstract long nextDegree();

	public double mean(long numPersons) {
		return -1;
	}
}
