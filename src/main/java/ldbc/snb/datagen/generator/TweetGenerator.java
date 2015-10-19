package ldbc.snb.datagen.generator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.dictionary.TextGenerator;
import ldbc.snb.datagen.objects.Person.PersonSummary;
import ldbc.snb.datagen.util.DistributionKey;
import ldbc.snb.datagen.util.RandomGeneratorFarm;



public class TweetGenerator extends TextGenerator {
	private DistributionKey hashtag;
	private DistributionKey sentiment;
	private DistributionKey popularword;
	private DistributionKey ponene; //distribution popular,negative, neutral tweets
	private DistributionKey lengthsentence; // sentece length and sentences per tweet
	private DistributionKey lengthtweet;

	public TweetGenerator(Random random, TagDictionary tagDic) throws NumberFormatException, IOException {
		super(random, tagDic);
		//input de fitxers i crea els 4 maps
		String dir = null;
		hashtag = new DistributionKey(dir);
		sentiment = new DistributionKey(dir);
		popularword = new DistributionKey(dir);
		ponene = new DistributionKey(dir);
		lengthsentence = new DistributionKey(dir);
		lengthtweet = new DistributionKey(dir);
		
		
		
	}

	@Override
	protected void load() {
		
	}

	@Override
	public String generateText(PersonSummary member, TreeSet<Integer> tags, Properties prop) {
		StringBuffer content = null;
		//mirar num de frases
		Double numsentences = Double.valueOf(lengthtweet.nextDouble(this.random));
		for (int i = 0; i < numsentences; ++i){
			Double numwords = Double.valueOf(lengthsentence.nextDouble(this.random));
			// depenen de la distribució de number hashtags per sentence int numhashtags;
			//int numhashtags = funciondistribuciohashtags(numwords);
			int numhashtags = 4;
			for (int j = 0; j<numhashtags; ++j){
				content.append(" "+ hashtag.nextDouble(this.random)); 
			}
			// depenen de la distribució de number sentiment words per sentence int numhashtags;
			//int numsentimentswords = funciondistribuciosentimentswords(numwords);
			int numsentimentswords = 4;
			for (int q = 0; q<numhashtags; ++q){
				content.append(" "+ sentiment.nextDouble(this.random)); 
			}
			numwords -= (numsentimentswords + numhashtags);
			for (int j = 0; j<numwords; ++j){
				content.append(" "+ popularword.nextDouble(this.random)); 
			}
			
		}
		//per cada frase mirar numero de paraules
		//mirar numero de hashtags
		content.toString();
		return content.toString();
		
	}


}
