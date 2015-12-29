package ldbc.snb.datagen.generator;

import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.dictionary.TextGenerator;
import ldbc.snb.datagen.objects.Person.PersonSummary;
import ldbc.snb.datagen.util.RandomGeneratorFarm;

public class LdbcSnbTextGenerator extends TextGenerator {

	public LdbcSnbTextGenerator(Random random, TagDictionary tagDic) {
		super(random, tagDic);
		
	}

	@Override
	protected void load() {
		
	}

	@Override
	public String generateText(PersonSummary member, TreeSet<Integer> tags, Properties prop) {
		//fem un if segons las props i fem una cosa o una altre
		String content = "";
		if(prop.getProperty("type") == "post"){//si es post fer
			
			int textSize;
			if( member.isLargePoster() && this.random.nextDouble() > (1.0f-DatagenParams.ratioLargePost) ) {
				textSize = Dictionaries.tagText.getRandomLargeTextSize( this.random,DatagenParams.minLargePostSize, DatagenParams.maxLargePostSize );
			} else {
				textSize = Dictionaries.tagText.getRandomTextSize( this.random, this.random, DatagenParams.minTextSize, DatagenParams.maxTextSize);
			}
			content = Dictionaries.tagText.generateText(this.random,tags,textSize);
		}
		else {//si no es post fer
			int textSize;
			if( member.isLargePoster() && this.random.nextDouble() > (1.0f-DatagenParams.ratioLargeComment) ) {
				textSize = Dictionaries.tagText.getRandomLargeTextSize(this.random, DatagenParams.minLargeCommentSize, DatagenParams.maxLargeCommentSize);
			} else {
				textSize = Dictionaries.tagText.getRandomTextSize( this.random, this.random, DatagenParams.minCommentSize, DatagenParams.maxCommentSize);
			}
			content = Dictionaries.tagText.generateText(this.random,tags,textSize);
		}
		return content;
	}

}
