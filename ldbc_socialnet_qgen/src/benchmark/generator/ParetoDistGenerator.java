package benchmark.generator;

import java.util.Random;

import umontreal.iro.lecuyer.probdist.ParetoDist;

public class ParetoDistGenerator {
  
	private final ParetoDist pareto;
	private final Random rand;
	
	public ParetoDistGenerator(double alpha, long seed) {
		pareto 	= new ParetoDist(alpha);
		rand = new Random(seed);
	}
	
	/**
	 * Return a value between from and to, with both inclusive
	 * @param from
	 * @param to
	 * @return
	 */
	public int getValue(int from, int to) {
		int value = 0;
		
		for(value = (int) pareto.inverseF(rand.nextDouble()); value > to || value < from; value = (int) pareto.inverseF(rand.nextDouble()))
		  continue;
		return value;
	}
	
}
