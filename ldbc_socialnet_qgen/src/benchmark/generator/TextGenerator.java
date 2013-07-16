package benchmark.generator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;

import benchmark.tools.NGram;

public class TextGenerator {
  
	BufferedReader dictionary;
	private Random ranGen;
	private HashMap<String,Integer> logList;//The word list for the Test Driver
	private Vector<String> words;//For faster access, save all words in Vector-Array
	
    public NGram gram = NGram.GRAM1; // dictionary of unigram terms by default
	private ParetoDistTermsGenerator pareto; // the Pareto terms distributed generator
	private final StringBuilder sb = new StringBuilder();
	
	private final static int FINDSTART = 1;
	private final static int READWORD = 2;
	private final static int FINISHED = 3;
	private final static int EOF = 4;

	public TextGenerator(String file)
	{
		ranGen = new Random();

		init(file);
	}
	
	public TextGenerator(String file, long seed)
	{
		ranGen = new Random(seed);
		
		init(file);
	}
	
	public TextGenerator(String file, long seed, NGram gram) {
	  this(file, seed);
	  this.gram = gram;
	}

	/**
	 * Sets the slope (i.e., the pareto index) of the Pareto distribution
	 * generator, according to the ngram
	 * @param gram
	 * @return
	 */
	private float getSlope(NGram gram) {
	  switch (gram) {
	    case GRAM1:
        return 1;
      case GRAM2:
        return 0.7f;
      case GRAM3:
        return 0.5f;
      case GRAM4:
        return 0.4f;
      case GRAM5:
        return 0.39f;
      default:
        return 1;
    }
	}
	
	//Initialize this TextGenerator
	private void init(String file) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(file);
        if (is==null) {
            System.err.println("Resource not found: "+file);
            System.exit(-1);
        }
        dictionary = new BufferedReader(new InputStreamReader(is));
        
        System.out.print("Reading in " + file + ": ");
		logList = null;
			
		createWordList();
		
		try {
			dictionary.close();
            pareto = new ParetoDistTermsGenerator(getSlope(gram), words, ranGen); // creates a Pareto generator over the words list
		} catch(IOException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
	}
	
	//Generates a Vector of words
	@SuppressWarnings("fallthrough")
    private void createWordList() {
		words = new Vector<String>();

		while(true) {
			StringBuffer word = new StringBuffer();
			try{
				int state = FINDSTART;
				char c=' ';
				
				while(state!=FINISHED)
				{
                    int res=dictionary.read();
                    if (res==-1) {
                        state = EOF;
                        break;
                    }
                    c=(char)res;
	
					switch(state)
					{
						case FINDSTART:
							if(isLetter(c))
								state = READWORD;//is a letter, go on with READWORD
						case READWORD:
							if(isLetter(c))
								word.append(c);
							else
								state = FINISHED;
					}
				}
				
				//Check if a word has been read in
				if(word.length()>0) {
					String wordString = word.toString();
					words.add(wordString);
				}
					
				//Finish at EOF
				if(state==EOF)
					break;
				
			} catch(IOException e) {
				System.err.println("Couldn't get word.\n"+e.getMessage());
			}
		}
		System.out.println(words.size() + " words read in.");
	}
	
	//reads a random word from the text file
	private String getRandomWord()
	{
		int index = ranGen.nextInt(words.size());
		String word = words.elementAt(index);

		if(logList!=null) 
			addWordToWordlist(word);
		
		return word;
	}
	
	private void addWordToWordlist(String word) {
		Integer count = 1;
		if(logList.containsKey(word)) {
			count = logList.get(word);
			count++;
		}
		logList.put(word, count);
	}
	
	/*
	 * returns a random sentence with number words from the chosen dictionary.
	 */
	public String getRandomSentence(int numberWords)
	{
		StringBuffer sentence = new StringBuffer();
		
		if(numberWords>0)
			sentence.append(getRandomWord());
		
		for(int i=1;i<numberWords;i++) {
			sentence.append(" ");
			sentence.append(getRandomWord());
		}
		
		return sentence.toString();
	}
	 
	/**
	 * Return a sentence of nb terms, which have a Pareto distribution
	 * @param nb the number of terms in the sentence
	 * @return
	 */
    public String getParetoSentence(final int nb) {
        if (nb == 0)
          return "";
        String term;
        
        pareto.reset(nb);
        sb.setLength(0);
        term = pareto.next();
        if(logList != null) 
          addWordToWordlist(term);
        sb.append(term);
      while (pareto.hasNext()) {
        term = pareto.next();
        if(logList != null) 
          addWordToWordlist(term);
          sb.append(" ").append(term);
        }
      return sb.toString();
   }
      
      /**
       * Return a term chosen with a Pareto probablity and add it to the frequency list
       * @return
       */
   public String getParetoWord() {
        final String term = pareto.getTerm();
        
      if(logList != null) 
        addWordToWordlist(term);
      return term;
   }
	
	private boolean isLetter(char c)
	{
		return Character.isLetter(c) || c=='-' || c == ' ';
	}
	
	/*
	 * Generate a homepage for a producer
	 */
	public static String getProducerWebpage(int producerNr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append("http://www.Producer");
		s.append(producerNr);
		s.append(".com/");
		
		return s.toString();
	}
	
	/*
	 * Generate a homepage for a vendor
	 */
	public static String getVendorWebpage(int vendorNr)
	{
		StringBuffer s = new StringBuffer();
		
		s.append("http://www.vendor");
		s.append(vendorNr);
		s.append(".com/");
		
		return s.toString();
	}
	
	public HashMap<String, Integer> getWordList() {
		return logList;
	}

	public void activateLogging(HashMap<String, Integer> logList) {
		this.logList = logList;
	}
	
	public void deactivateLogging() {
		this.logList = null;
	}
}
