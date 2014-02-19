/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.socialnet.dbgen.dictionary;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Random;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.generator.ScalableGenerator;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;


public class TagTextDictionary {

	
    public static int commentId = -1;
    
    private static final String SEPARATOR = "  ";
    
    String dicFileName;
    DateGenerator dateGen;
    
    TagDictionary tagDic;
	HashMap<Integer, String> tagText;
	
	Random rand;
	Random randTextSize;
	Random randReducedText;
    double reducedTextRatio;
	
	public TagTextDictionary(String dicFileName, DateGenerator dateGen, TagDictionary tagDic, 
	        double reduceTextRatio, long seed, long seedTextSize ){
		this.dicFileName = dicFileName;
		this.tagText = new HashMap<Integer, String>();
		this.dateGen = dateGen;
		this.tagDic = tagDic;
		this.rand = new Random(seed);
		this.randReducedText = new Random(seed);
		this.randTextSize = new Random(seedTextSize);
		this.reducedTextRatio = reducedTextRatio;
	}
	
	public void initialize() {
	    try {
	        BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(dicFileName), "UTF-8"));
	        String line;
	        while ((line = dictionary.readLine()) != null){
	            String[] data = line.split(SEPARATOR);
	            Integer id = Integer.valueOf(data[0]);
	            tagText.put(id, data[1]);
	        }
	        dictionary.close();
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	public String getTagText(int id) {
	    return tagText.get(id);
	}

    public String getRandomText(TreeSet<Integer> tags, int minSize, int maxSize ) {

        int textSize;
        int startingPos;
        String returnString = "";
        
        // Generate random fragment from the content 
        if (randReducedText.nextDouble() > reducedTextRatio){
            textSize = randTextSize.nextInt(maxSize - minSize) + minSize;
        }
        else{
            textSize = randTextSize.nextInt((maxSize >> 1) - minSize) + minSize;
        }

        int textSizePerTag = textSize / tags.size();
        Iterator<Integer> it = tags.iterator();
        while (it.hasNext()) {
            Integer tag = it.next();
            String content = getTagText(tag);
            if (textSizePerTag >= content.length()) {
                returnString += content;
            } else {
                startingPos = randTextSize.nextInt(content.length() - textSizePerTag);
                String finalString = content.substring(startingPos, startingPos + textSizePerTag - 1);
                
                String tagName = tagDic.getName(tag).replace("_", " ");
                tagName = tagName.replace("\"", "\\\"");
                String prefix = "About " +tagName+ ", ";

                int posSpace = finalString.indexOf(" ");
                returnString += (posSpace != -1) ? prefix + finalString.substring(posSpace).trim() : prefix + finalString;
                posSpace = returnString.lastIndexOf(" ");
                if (posSpace != -1){
                    returnString = returnString.substring(0, posSpace);
                }
            }
            if (!returnString.endsWith(".")) {
                returnString =  returnString + ".";
            }
            if (it.hasNext()) {
                returnString += " ";
            }
        }
        return returnString.replace("|", " ");
    }

    public String getRandomLargeText(TreeSet<Integer> tags, int minSize, int maxSize) {
       int textSize = rand.nextInt(maxSize - minSize) + minSize;
       String content = new String(); 
       Iterator<Integer> it = tags.iterator();
       while(content.length() < textSize) {
        if (!it.hasNext()){
            it = tags.iterator();
        }
        Integer tag = it.next();
        String tagContent = getTagText(tag);
        if( content.length() + tagContent.length() < textSize) {
            content = content.concat(tagContent);
        } else {
            content = content.concat(tagContent.substring(0,textSize - content.length()));
        }
    }
    return content;
}


}
