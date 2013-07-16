/**
 * Copyright (C) 2011, Campinas Stephane This program is free software: you can
 * redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the
 * License, or any later version. This program is distributed in the hope that
 * it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 */
/**
 * @author Campinas Stephane [ 3 May 2011 ]
 * @link stephane.campinas@deri.org
 */
package benchmark.generator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import umontreal.iro.lecuyer.probdist.ParetoDist;

/**
 * Provides an iterator over a stream of n terms having a Pareto distribution
 */
public class ParetoDistTermsGenerator implements Iterator<String> {
  
  private final Random              rand;
  
  private final ParetoDist          pareto;
  private final List<String>        terms;
  private int                       length;
  
  public ParetoDistTermsGenerator(final double alpha, final List<String> words, final Random rand)
  throws IOException {
    this.rand = rand;
    pareto = new ParetoDist(alpha);
    terms = words;
  }

  public void reset(final int n) {
    length = n;
  }
  
  public String getTerm() {
    return terms.get(paretoIndex(terms.size()));
  }
  
  /**
   * Return an integer between 0 and n, exclusive
   * @param n
   * @return
   */
  private int paretoIndex(int n) {
    int index;
    
    for (index = (int) pareto.inverseF(rand.nextDouble()) - 1; index >= n; index = (int) pareto.inverseF(rand.nextDouble()) - 1)
      continue;
    return index;
  }
  
  @Override
  public boolean hasNext() {
    return length != 0;
  }

  @Override
  public String next() {
    length--;
    return terms.get(paretoIndex(terms.size()));
  }

  @Override
  public void remove() {
  }
  
}
