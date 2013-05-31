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
package ldbc.socialnet.dbgen.generator;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.UserAgentDictionary;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.UserProfile;


public class RandomTextGenerator {
	private String locationArticlefileName;
	private String interestArticlefileName;
	private String stopWordFileName;
	
	BufferedReader regionalDictionary;
	BufferedReader interestDictionary;
	BufferedReader stopWordDictionary;
	
	Random rand;				// This random generator is used for generating 
								// friendIdx, articleIdx
	Random randTextSize;
	
	Random randGroupPost; 		// For selecting whether it is for interest or for location
	double groupPostInterestProb = 0.8; 
	
	Vector<Vector<String>> locationArticles;
	Vector<Vector<String[]>> locationArticleTags;
	
	Vector<Vector<String>> interestArticles;
	Vector<Vector<String[]>> interestArticleTags;	
	
	HashMap<String, Integer> 	locationNames;
	HashMap<String, Integer> 	interestNames;
	HashSet<String> 			stopWords;
	
	DateGenerator dateGen;
	int numberOfRegionalArticles;
	int numberOfInterestArticles;
	
//	static int postId = -1;
	static int commentId = -1;
	
	int minSizeOfText;
	int maxSizeOfText;
	int minSizeOfComment; 
	int maxSizeOfComment; 
	int reduceTextSize; 
	int reduceCommentSize; 
	
	double reduceTextRatio; 
	Random randReduceText; 
	Random randReplyTo; 			//For comment
	
	UserAgentDictionary userAgentDic; 
	IPAddressDictionary ipAddDic;
	BrowserDictionary browserDic;

	public RandomTextGenerator(String _regionalFileName, String _interestFileName, String _stopWordFileName,
			long seed, long seedTextSize, int docNumber, 
			HashMap<String, Integer> _locationNames, HashMap<String, Integer> _interestNames,
			DateGenerator _dateGen,
			int _minSizeOfText, int _maxSizeOfText,
			int _minSizeOfComment, int _maxSizeOfComment, double _reduceTextRatio, long seedRandReduceText) {
		
		numberOfRegionalArticles = -1;
		rand = new Random(seed);
		randTextSize = new Random(seedTextSize);
		this.locationArticlefileName = _regionalFileName;
		this.interestArticlefileName = _interestFileName;
		this.stopWordFileName = _stopWordFileName;
		
		this.locationNames = _locationNames;
		this.interestNames = _interestNames;
		
		this.dateGen = _dateGen;
		
		this.minSizeOfText = _minSizeOfText;
		this.maxSizeOfText = _maxSizeOfText;
		this.minSizeOfComment = _minSizeOfComment;
		this.maxSizeOfComment = _maxSizeOfComment;
		this.reduceTextSize = maxSizeOfText >> 1;
		this.reduceCommentSize = maxSizeOfComment >> 1; 
		this.reduceTextRatio = _reduceTextRatio;
		
		randReduceText = new Random(seedRandReduceText); 
		randReplyTo = new Random(seed);
		randGroupPost = new Random(seed);
		
		// Only store the articles whose locations or interests are in the dictionary 
		locationArticles = new Vector<Vector<String>>(locationNames.size());
		locationArticleTags = new Vector<Vector<String[]>>(locationNames.size());
		for (int i = 0; i < locationNames.size(); i++){
			locationArticles.add(new Vector<String>());
			locationArticleTags.add(new Vector<String[]>());
		}

		interestArticles = new Vector<Vector<String>>(interestNames.size());
		interestArticleTags = new Vector<Vector<String[]>>(interestNames.size());
		for (int i = 0; i < interestNames.size(); i++){
			interestArticles.add(new Vector<String>());
			interestArticleTags.add(new Vector<String[]>());
		}
		
		stopWordsInit();
		regionalInit(docNumber);
		interestInit(docNumber);
		//checking();
	}
	
	public void setSupportDictionaries(UserAgentDictionary _userAgentDic, IPAddressDictionary _ipAddDic,
									   BrowserDictionary _browserDic){
		this.userAgentDic = _userAgentDic; 
		this.ipAddDic = _ipAddDic;
		this.browserDic = _browserDic;
	}
	
	public void checking(){
		for (int i = 0; i < locationArticles.size(); i++){
			if (locationArticles.get(i).size() == 0){
				System.out.println("Location " + i + " does not have any articles ");
			}
		}
                for (int i = 0; i < interestArticles.size(); i++){
                        if (interestArticles.get(i).size() == 0){
                                System.out.println("Interest " + i + " does not have any articles ");
                        }
                }

	}

	public void regionalInit(int docNumber) {
		try {
			regionalDictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(locationArticlefileName), "UTF-8"));

			regionalTextExtract();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void stopWordsInit() {
		try {
			stopWordDictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(stopWordFileName), "UTF-8"));
			stopWords = new HashSet<String>();
			String word; 
			while ((word = stopWordDictionary.readLine()) != null){
				stopWords.add(word.trim());
			}
			stopWordDictionary.close();
			
			System.out.println(stopWords.size() + " stop words extracted ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void interestInit(int docNumber) {
		try {
			interestDictionary = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(interestArticlefileName), "UTF-8"));
			interestTextExtract();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/*
	 * Get the offsets of each article in the input file of dictionary
	 */
	public void regionalTextExtract() {
		String strLine = "";
		String curLocation = "";
		String onlyCharacterStr;
		int curLocationIdx = -1;
		System.out.println("Extracting regional articles ");
		int lineNo = 0;
		try {
			while ((strLine = regionalDictionary.readLine()) != null) {
				lineNo++;
				String infos[] = strLine.split("  ");
				
				if (locationNames.containsKey(infos[0])){
					if (curLocation.compareTo(infos[0]) != 0) {
						curLocationIdx = locationNames.get(infos[0]);
						curLocation = infos[0];
						onlyCharacterStr = infos[1].replaceAll("[^A-Za-z0-9 ]", "");
						String[] tags = onlyCharacterStr.split(" ");
						Vector<String> tagsWithoutStopWord = new Vector<String>();
						for (int i = 0; i < tags.length; i++){
							if (!stopWords.contains(tags[i]) && tags[i].compareTo("")!=0)
								tagsWithoutStopWord.add(tags[i].trim());
						}
						
						String[] finalTags = new String[tagsWithoutStopWord.size()]; 
						tagsWithoutStopWord.toArray(finalTags);
						locationArticleTags.get(curLocationIdx).add(finalTags);
						locationArticles.get(curLocationIdx).add(infos[2]);
					} else {
						onlyCharacterStr = infos[1].replaceAll("[^A-Za-z0-9 ]", "");
						String[] tags = onlyCharacterStr.split(" ");
						Vector<String> tagsWithoutStopWord = new Vector<String>();
						for (int i = 0; i < tags.length; i++){
							if (!stopWords.contains(tags[i]) && tags[i].compareTo("")!=0)
								tagsWithoutStopWord.add(tags[i]);
						}
						
						String[] finalTags = new String[tagsWithoutStopWord.size()]; 
						tagsWithoutStopWord.toArray(finalTags);
						locationArticleTags.get(curLocationIdx).add(finalTags);
						locationArticles.get(curLocationIdx).add(infos[2]);
					}
				}
			}
			
			//System.out.println("Size of the second location articles: " + locationArticles.get(1).size());
		} catch (Exception e) {
			System.out.println("Problem at line " + lineNo);
			e.printStackTrace();
		}
	}



	public void interestTextExtract() {
		String strLine = "";
		String curInterest = "";
		String onlyCharacterStr; 
		int curInterestIdx = -1; 
		System.out.println("Extracting interest-related articles ");
		int lineNo = 0;
		try {
			while ((strLine = interestDictionary.readLine()) != null) {
				lineNo++;
				String infos[] = strLine.split("  ");
				if (interestNames.containsKey(infos[0].toLowerCase())){
					if (curInterest.compareTo(infos[0]) != 0) {
						curInterestIdx = interestNames.get(infos[0].toLowerCase());
						curInterest = infos[0];
						onlyCharacterStr = infos[1].replaceAll("[^A-Za-z0-9 ]", "");
						String[] tags = onlyCharacterStr.split(" ");
						Vector<String> tagsWithoutStopWord = new Vector<String>();
						for (int i = 0; i < tags.length; i++){
							if (!stopWords.contains(tags[i]) && tags[i].compareTo("")!=0)
								tagsWithoutStopWord.add(tags[i]);
						}
						
						String[] finalTags = new String[tagsWithoutStopWord.size()]; 
						tagsWithoutStopWord.toArray(finalTags);
						interestArticleTags.get(curInterestIdx).add(finalTags);
					
						interestArticles.get(curInterestIdx).add(infos[2]);
					} else {
						onlyCharacterStr = infos[1].replaceAll("[^A-Za-z0-9 ]", "");
						String[] tags = onlyCharacterStr.split(" ");
						Vector<String> tagsWithoutStopWord = new Vector<String>();
						for (int i = 0; i < tags.length; i++){
							if (!stopWords.contains(tags[i]) && tags[i].compareTo("")!=0)
								tagsWithoutStopWord.add(tags[i]);
						}
						
						String[] finalTags = new String[tagsWithoutStopWord.size()]; 
						tagsWithoutStopWord.toArray(finalTags);
						interestArticleTags.get(curInterestIdx).add(finalTags);
						interestArticles.get(curInterestIdx).add(infos[2]);
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Problem at line " + lineNo);
			e.printStackTrace();
		}
	}
	/*
	public String getRandomRegionalTextUsingOffset() {
		String content;
		// Randomly get the index of a article
		int idx = rand.nextInt(numberOfRegionalArticles - 1);
		long offset = regionalDocOffset.get(idx);

		int textSize;
		int startingPos;

		String finalString = "";

		try {
			regionalDictionary.seek(offset);
			content = regionalDictionary.readLine();

			// Generate random fragment from the content
			textSize = rand.nextInt(maxSizeOfText - minSizeOfText)
					+ minSizeOfText;

			if (textSize >= content.length()) {
				return content;
			} else {
				// Get the starting position for the fragment of text
				startingPos = rand.nextInt(content.length() - textSize - 1);
				finalString = content.substring(startingPos, startingPos
						+ textSize);
				return finalString.substring(finalString.indexOf(" ") + 1,
						finalString.lastIndexOf(" "));
			}

		} catch (IOException e) {

		}

		return "";
	}
	*/

	public int getRegionalArticleIdx(int locationIdx) {
		try {
			return rand.nextInt(locationArticles.get(locationIdx).size());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Location Idx = " + locationIdx );
			System.exit(-1);
		}
		return rand.nextInt(locationArticles.get(locationIdx).size());
	}
	public int getInterestArticleIdx(int interestIdx) {
		return rand.nextInt(interestArticles.get(interestIdx).size());
	}
	
	public String getRandomRegionalText(int locationIdx, int articleIdx) {
		String content;

		int textSize;
		int startingPos;

		String finalString = "";

		content = locationArticles.get(locationIdx).get(articleIdx);
		
		// Generate random fragment from the content 
		if (randReduceText.nextDouble() > reduceTextRatio){
			textSize = randTextSize.nextInt(maxSizeOfText - minSizeOfText)
				+ minSizeOfText;
		}
		else{
			textSize = randTextSize.nextInt(reduceTextSize - minSizeOfText)
			+ minSizeOfText;
		}

		if (textSize >= content.length()) {
			return content;
		} else {
			// Get the starting position for the fragment of text
			startingPos = randTextSize.nextInt(content.length() - textSize);
			finalString = content
					.substring(startingPos, startingPos + textSize - 1);
			
			/*
			return finalString.substring(finalString.indexOf(" ") + 1,
					finalString.lastIndexOf(" "));
					*/
			return returnWholeWords(finalString);
		}
	}
	
	public String getRandomInterestText(int interestIdx, int articleIdx) {
		String content;

		int textSize;
		int startingPos;

		String finalString = "";

		content = interestArticles.get(interestIdx).get(articleIdx);

		// Generate random fragment from the content
		if (randReduceText.nextDouble() > reduceTextRatio){
			textSize = randTextSize.nextInt(maxSizeOfText - minSizeOfText)
				+ minSizeOfText;
		}
		else{
			textSize = randTextSize.nextInt(reduceTextSize - minSizeOfText)
			+ minSizeOfText;
		}

		if (textSize >= content.length()) {
			return content;
		} else {
			// Get the starting position for the fragment of text
			startingPos = randTextSize.nextInt(content.length() - textSize);
			finalString = content
					.substring(startingPos, startingPos + textSize - 1);
			
			/*
			return finalString.substring(finalString.indexOf(" ") + 1,
					finalString.lastIndexOf(" "));
					*/
			return returnWholeWords(finalString);
		}
	}
	
	public String getRandomRegionalComment(int locationIdx, int articleIdx) {
		String content;

		int textSize;
		int startingPos;

		String finalString = "";

		content = locationArticles.get(locationIdx).get(articleIdx);

		// Generate random fragment from the content
		if (randReduceText.nextDouble() > reduceTextRatio){
			textSize = randTextSize.nextInt(maxSizeOfComment - minSizeOfComment)
											+ minSizeOfComment;
		}
		else{
			textSize = randTextSize.nextInt(reduceCommentSize - minSizeOfComment)
												+ minSizeOfComment;
		}


		if (textSize >= content.length()) {
			return content;
		} else {
			// Get the starting position for the fragment of text
			startingPos = randTextSize.nextInt(content.length() - textSize);
			finalString = content
					.substring(startingPos, startingPos + textSize - 1);
			
			/*
			return finalString.substring(finalString.indexOf(" ") + 1,
					finalString.lastIndexOf(" "));
					*/
			return returnWholeWords(finalString);
		}
	}	
	
	public String getRandomInterestComment(	int interestIdx, int articleIdx) {
		String content;

		int textSize;
		int startingPos;

		String finalString = "";

		content = interestArticles.get(interestIdx).get(articleIdx);

		// Generate random fragment from the content
		if (randReduceText.nextDouble() > reduceTextRatio){
			textSize = randTextSize.nextInt(maxSizeOfComment - minSizeOfComment)
											+ minSizeOfComment;
		}
		else{
			textSize = randTextSize.nextInt(reduceCommentSize - minSizeOfComment)
												+ minSizeOfComment;
		}

		if (textSize >= content.length()) {
			return content;
		} else {
			// Get the starting position for the fragment of text
			startingPos = randTextSize.nextInt(content.length() - textSize);
			finalString = content
					.substring(startingPos, startingPos + textSize - 1);
			
			/*
			return finalString.substring(finalString.indexOf(" ") + 1,
					finalString.lastIndexOf(" "));
					*/
			return returnWholeWords(finalString);
		}
	}
	public String returnWholeWords(String inputString){
		int posSpace = inputString.indexOf(" ");
		String returnString;
		if (posSpace != -1)
			returnString = inputString.substring(posSpace).trim();
		else
			returnString = inputString;
		
		posSpace = returnString.lastIndexOf(" ");
		
		if (posSpace != -1){
			returnString = returnString.substring(0, posSpace);
		}
		/* Do not need to consider
		if (returnString.indexOf("\"") != -1){
			//System.out.println("HAPPENS ");
			returnString = returnString.replace("\"","");
			//System.out.println(returnString);
			//System.exit(-1);
		}
		*/
		return returnString;
	}
	public String[] getRegionalTags(int locationIdx, int articleIdx) {

		return locationArticleTags.get(locationIdx).get(articleIdx);
	}
	
	public String[] getInterestTags(int interestIdx, int articleIdx) {

		return interestArticleTags.get(interestIdx).get(articleIdx);
	}	
	
	public int[] getRegionalFriendsLiked(ReducedUserProfile user, int numOfLikes){
		Friend fullFriendList[] = user.getFriendList();
		int numLocationFriends = user.getLastLocationFriendIdx() + 1;
		
		// For randomly selecting list of friends liked the post
		// we only random select a start index and then collect all the friendId from 
		// that start idx
		
		int friends[];
		if (numOfLikes >= numLocationFriends){
			friends = new int[numLocationFriends];
			for (int j = 0; j < numLocationFriends; j++){
				friends[j] = fullFriendList[j].getFriendAcc();
			}
		}
		else{
			friends = new int[numOfLikes];
			int startIdx = rand.nextInt(numLocationFriends - numOfLikes);
			for (int j = 0; j < numOfLikes; j++){
				friends[j] = fullFriendList[j+startIdx].getFriendAcc();
			}			
		}
		
		return friends; 
	}
	
	public int[] getInterestFriendsLiked(ReducedUserProfile user, int numOfLikes){
		Friend fullFriendList[] = user.getFriendList();
		int startInterestFriendIdx = user.getStartInterestFriendIdx();
		int lastInterestFriendIdx = user.getLastInterestFriendIdx();
		int numInterestFriends = lastInterestFriendIdx - startInterestFriendIdx + 1;
		// For randomly selecting list of friends liked the post
		// we only random select a start index and then collect all the friendId from 
		// that start idx
		
		int friends[];
		if (numOfLikes >= numInterestFriends){
			friends = new int[numInterestFriends];
			for (int j = 0; j < numInterestFriends; j++){
				friends[j] = fullFriendList[j+startInterestFriendIdx].getFriendAcc();
			}
		}
		else{
			friends = new int[numOfLikes];
			int startIdx = rand.nextInt(numInterestFriends - numOfLikes);
			for (int j = 0; j < numOfLikes; j++){
				friends[j] = fullFriendList[j+startIdx+startInterestFriendIdx].getFriendAcc();
			}			
		}
		
		return friends; 
	}
	
	public int[] getMembersLiked(Group group, int numOfLikes){
		GroupMemberShip groupMembers[] = group.getMemberShips();

		int numAddedMember = group.getNumMemberAdded();
		int friends[];
		if (numOfLikes >= numAddedMember){
			friends = new int[numAddedMember];
			for (int j = 0; j < numAddedMember; j++){
				friends[j] = groupMembers[j].getUserId();
			}
		}
		else{
			friends = new int[numOfLikes];
			int startIdx = rand.nextInt(numAddedMember - numOfLikes);
			for (int j = 0; j < numOfLikes; j++){
				friends[j] = groupMembers[j+startIdx].getUserId();
			}			
		}
		
		return friends; 
	}
	
	public Post getRandomRegionalPost(ReducedUserProfile user, int maxNumberOfLikes) {
		
		ScalableGenerator.postId++;
		// String tags[] =
		Post post = new Post();
		int articleIdx = getRegionalArticleIdx(user.getLocationIdx());
		post.setArticleIdx(articleIdx);
		
		post.setAuthorId(user.getAccountId());
		post.setContent(getRandomRegionalText(user.getLocationIdx(), articleIdx));
		post.setCreatedDate(dateGen.randomPostCreatedDate(user));

		post.setForumId(user.getAccountId() * 2); // Temporarily use this value for forumId
		post.setPostId(ScalableGenerator.postId);
		
		//post.setTags(getRegionalTags(user.getLocationIdx(), articleIdx));
		HashSet<Integer> tags = new HashSet<Integer>();
		Iterator<Integer> it = user.getSetOfTags().iterator();
		while (it.hasNext()) {
		    Integer value = it.next();
		    if (tags.isEmpty()) {
		        tags.add(value);
		    } else {
		        if (rand.nextDouble() < 0.2) {
		            tags.add(value);
		        }
		    }
		}
		post.setTags(tags);
		
		int numberOfLikes = rand.nextInt(maxNumberOfLikes);
		
        int[] likes = getRegionalFriendsLiked(user, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.sevenDayInMillis+post.getCreatedDate());
        }
        post.setInterestedUserAccsTimestamp(likeTimestamp);
		
		// Get random comments
		
		return post;
	}
	
	public Post getRandomInterestPost(ReducedUserProfile user, int maxNumberOfLikes) {
		
		ScalableGenerator.postId++;
		// String tags[] =
		Post post = new Post();
		HashSet<Integer> interestSet = user.getSetOfInterests();
		
		// Randomly select one interest
		Iterator iter = interestSet.iterator();
		int idx = rand.nextInt(interestSet.size());
		for (int i = 0; i < idx; i++){
			iter.next();
		}
		  
		int interestIdx = ((Integer)iter.next()).intValue();
		post.setInterestIdx(interestIdx);
		int articleIdx = getInterestArticleIdx(interestIdx);
		post.setArticleIdx(articleIdx);
		
		post.setAuthorId(user.getAccountId());
		post.setContent(getRandomInterestText(interestIdx, articleIdx));
		
		post.setCreatedDate(dateGen.randomPostCreatedDate(user));

		post.setForumId(user.getAccountId() * 2); // Temporarily use this value
													// for forumId
		post.setPostId(ScalableGenerator.postId);
		
		//post.setTags(getInterestTags(interestIdx, articleIdx));
		HashSet<Integer> tags = new HashSet<Integer>();
        Iterator<Integer> it = user.getSetOfTags().iterator();
        while (it.hasNext()) {
            Integer value = it.next();
            if (tags.isEmpty()) {
                tags.add(value);
            } else {
                if (rand.nextDouble() < 0.2) {
                    tags.add(value);
                }
            }
        }
        post.setTags(tags);
		
		int numberOfLikes = rand.nextInt(maxNumberOfLikes);
		
		int[] likes = getInterestFriendsLiked(user, numberOfLikes);
		post.setInterestedUserAccs(likes);
		long[] likeTimestamp = new long[likes.length];
		for (int i = 0; i < likes.length; i++) {
		    likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.sevenDayInMillis+post.getCreatedDate());
		}
		post.setInterestedUserAccsTimestamp(likeTimestamp);
		
		
		// Get random comments
		
		return post;
	}	
	
	public Post getRandomGroupPost(Group group, int maxNumberOfLikes) {
		
		ScalableGenerator.postId++;

		Post post = new Post();
		
		// Get one authorId
		//int memberIdx = rand.nextInt(group.getMemberShips().length);
		int memberIdx = rand.nextInt(group.getNumMemberAdded());
		
		GroupMemberShip memberShip = group.getMemberShips()[memberIdx];
		
		post.setAuthorId(memberShip.getUserId());
		
		int interestIdx = group.getInterestIdx();
		int locationIdx = group.getLocationIdx();
		post.setInterestIdx(interestIdx);
		int articleIdx;
		if (randGroupPost.nextDouble() < groupPostInterestProb){
			articleIdx = getInterestArticleIdx(interestIdx);
			post.setContent(getRandomInterestText(interestIdx, articleIdx));
			//post.setTags(getInterestTags(interestIdx, articleIdx));
			post.setInterestPost(true);
		}
		else{ 
			articleIdx = getRegionalArticleIdx(locationIdx);
			post.setContent(getRandomRegionalText(locationIdx, articleIdx));
			//post.setTags(getRegionalTags(locationIdx, articleIdx));
			post.setInterestPost(false);
		}
		HashSet<Integer> tags = new HashSet<Integer>();
		for (int i = 0; i < group.getTags().length; i++) {
		    tags.add(group.getTags()[i]);
		}
		post.setTags(tags);
		
		
		post.setArticleIdx(articleIdx);
		
		post.setCreatedDate(dateGen.randomGroupPostCreatedDate(memberShip.getJoinDate()));

		post.setForumId(group.getForumWallId());
		
		post.setPostId(ScalableGenerator.postId);
		
		int numberOfLikes = rand.nextInt(maxNumberOfLikes);
        int[] likes = getMembersLiked(group, numberOfLikes);
        post.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.sevenDayInMillis+post.getCreatedDate());
        }
        post.setInterestedUserAccsTimestamp(likeTimestamp);
		
		return post;
	}	
	
	// The content of the commment is generated 
	// from the article containing the post
	public Comment getRandomRegionalComment(Post post, ReducedUserProfile user, long lastCommentCreatedDate,
								long startCommentId, long lastCommentId){
		
		Comment comment = new Comment();
		
		// For userId, randomly select from one of the friends
		
		int friendIdx = -1;
		
		if (user.getLastLocationFriendIdx() == -1){		// No location-related friends
			comment.setAuthorId(-1);
			return comment;
		}
		else 
			friendIdx = rand.nextInt(user.getLastLocationFriendIdx() + 1);
		
		// Only friend whose the friendship created before the 
		// createdDate of the post gives the comment
		Friend friend = user.getFriendList()[friendIdx]; 
		if ((friend.getCreatedTime() > post.getCreatedDate()) || (friend.getCreatedTime() == -1)){
			comment.setAuthorId(-1);
			return comment;
		}
		
		
		commentId++;
		comment.setAuthorId(friend.getFriendAcc());
		comment.setCommentId(commentId);
		comment.setPostId(post.getPostId());
		comment.setReply_of(getReplyToId(startCommentId, lastCommentId));
		comment.setForumId(post.getForumId());

		userAgentDic.setCommentUserAgent(friend, comment);
		ipAddDic.setCommentIPAdress(friend.isFrequentChange(), friend.getSourceIp(), comment);
		comment.setBrowserIdx(browserDic.getPostBrowserId(friend.getBrowserIdx()));

		
		comment.setContent(getRandomRegionalComment(user.getLocationIdx(), post.getArticleIdx()));
		
		comment.setCreateDate(dateGen.powerlawCommDateDay(lastCommentCreatedDate));
		
		return comment;
	}
	
	public Comment getRandomInterestComment(Post post, ReducedUserProfile user, long lastCommentCreatedDate,
											long startCommentId, long lastCommentId){
		
		Comment comment = new Comment();
		
		// For userId, randomly select from one of the friends
		int friendIdx;
		if (user.getLastInterestFriendIdx() >= user.getStartInterestFriendIdx()){
			friendIdx = rand.nextInt(user.getLastInterestFriendIdx() - user.getStartInterestFriendIdx() + 1) + (user.getStartInterestFriendIdx());
		}
		else{
			friendIdx = -1;
			comment.setAuthorId(-1);
			return comment;
		}
		// Only friend whose the friendship created before the 
		// createdDate of the post gives the comment
		Friend friend = user.getFriendList()[friendIdx]; 
		if ((friend.getCreatedTime() > post.getCreatedDate()) || (friend.getCreatedTime() == -1)){
			comment.setAuthorId(-1);
			return comment;
		}
		
		commentId++;
		comment.setAuthorId(friend.getFriendAcc());
		comment.setCommentId(commentId);
		comment.setPostId(post.getPostId());
		comment.setReply_of(getReplyToId(startCommentId, lastCommentId));
		comment.setForumId(post.getForumId());

		userAgentDic.setCommentUserAgent(friend, comment);
		ipAddDic.setCommentIPAdress(friend.isFrequentChange(), friend.getSourceIp(), comment);
		comment.setBrowserIdx(browserDic.getPostBrowserId(friend.getBrowserIdx()));
		
		comment.setContent(getRandomInterestComment(post.getInterestIdx(), post.getArticleIdx()));
		
		comment.setCreateDate(dateGen.powerlawCommDateDay(lastCommentCreatedDate));
		
		return comment;
	}
	
	
	public Comment getRandomGroupComment(Post post, Group group, long lastCommentCreatedDate,
											long startCommentId, long lastCommentId){
		
		Comment comment = new Comment();
		
		// Randomly select one group member
		
		//int memberIdx = rand.nextInt(group.getMemberShips().length);
		int memberIdx = rand.nextInt(group.getNumMemberAdded());
		
		GroupMemberShip memberShip = group.getMemberShips()[memberIdx];

		if (memberShip.getJoinDate() > post.getCreatedDate()){
			comment.setAuthorId(-1);
			return comment;
		}
		
		commentId++;
		comment.setAuthorId(memberShip.getUserId());
		comment.setCommentId(commentId);
		comment.setPostId(post.getPostId());
		comment.setReply_of(getReplyToId(startCommentId, lastCommentId));
		comment.setForumId(post.getForumId());
		
		
		if (post.isInterestPost())
			comment.setContent(getRandomInterestComment(post.getInterestIdx(), post.getArticleIdx()));
		else
			comment.setContent(getRandomRegionalComment(group.getLocationIdx(), post.getArticleIdx()));
		
		
		comment.setCreateDate(dateGen.powerlawCommDateDay(lastCommentCreatedDate));
		
		return comment;
	}
	
		
	public long getReplyToId(long startId, long lastId){
		int parentId; 
		if (lastId > (startId+1)){
			parentId = randReplyTo.nextInt((int)(lastId - startId));
			if (parentId == 0) return -1; 
			else return (long)(parentId + startId); 
		}
		
		return -1; 
	}
	
	public UserAgentDictionary getUserAgentDic() {
		return userAgentDic;
	}

	public void setUserAgentDic(UserAgentDictionary userAgentDic) {
		this.userAgentDic = userAgentDic;
	}

	public IPAddressDictionary getIpAddDic() {
		return ipAddDic;
	}

	public void setIpAddDic(IPAddressDictionary ipAddDic) {
		this.ipAddDic = ipAddDic;
	}

	public BrowserDictionary getBrowserDic() {
		return browserDic;
	}

	public void setBrowserDic(BrowserDictionary browserDic) {
		this.browserDic = browserDic;
	}
}
