/**
 * Copyright 2011, Campinas Stephane
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * @project bsbmtools
 * @author Campinas Stephane [ 4 May 2011 ]
 * @link stephane.campinas@deri.org
 */
package benchmark.generator;

import java.io.IOException;

/**
 * 
 */
public class ReviewTermsGenerator {

  /* Review in increasing order of positive feedback: review5 are the most positive terms */
  private final String[]  reviewDicts     = { "review5-dict",
                                              "review5-dict",
                                              "review5-dict",
                                              "review5-dict",
                                              "review5-dict",
                                              "review5-dict",
                                              "review5-dict",
                                              "review5-dict",
                                              "review5-dict",
                                              "review5-dict" };
  private final float     treshold        = 0.1f; // each term has a probability of #treshold to be inserted  
  private final TextGenerator[]     reviewTerms      = new TextGenerator[10];
  
  private final StringBuilder   sentence = new StringBuilder();
  
  /**
   * @throws IOException 
   * 
   */
  public ReviewTermsGenerator() {
    for (int i = 0; i < 10; i++) {
      reviewTerms[i] = new TextGenerator(reviewDicts[i], Generator.seedGenerator.nextLong());
    }
  }
  
  public String getReviewSentence(final Integer[] scores, final int from, final int to, final ValueGenerator gen, final ParetoDistGenerator pareto) {
    int nWords = pareto.getValue(from, to);
    
    sentence.setLength(0);
    if (nWords == 1) {
      sentence.append(Generator.dictionary1.getParetoWord());
    } else {
      for (int len = 0; len < nWords; ) {
        if (gen.randomDouble(0, 1) < 0.5) {
          len++;
          sentence.append(Generator.dictionary1.getParetoWord()).append(' ');
        } else {
          len += 2;
          sentence.append(Generator.dictionary2.getParetoWord()).append(' ');
        }
      }      
    }
    // Add specific review terms with a probability of #treshold
    addScoreTerm(scores, gen);
    return sentence.toString().trim();
  }
  
  private void addScoreTerm(final Integer[] scores, final ValueGenerator gen) {
    for (Integer score : scores) {
      if (score != null && gen.randomDouble(0, 1) <= treshold) {
        sentence.append(reviewTerms[score - 1].getRandomSentence(1)).append(' ');
      }
    }
  }
  
}
