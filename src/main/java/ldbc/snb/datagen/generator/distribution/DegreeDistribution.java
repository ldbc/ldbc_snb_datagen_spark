/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ldbc.snb.datagen.generator.distribution;

/**
 *
 * @author aprat
 */
public interface DegreeDistribution {

	public void initialize();

	public void reset(long seed);

	public long nextDegree();
	
}
