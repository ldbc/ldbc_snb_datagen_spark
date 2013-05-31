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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.Photo;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.UserProfile;


public class UserAgentDictionary {
	String 				agentFileName = "";
	Vector<String> 		vUserAgents;
	BufferedReader  	agentFile; 
	Random				randGen;
	double 				probSentFromAgent; 
	Random				randSentFrom;
	
	public UserAgentDictionary(String _agentFileName, long seed, long seed2, double _probSentFromAgent){
		this.agentFileName = _agentFileName; 
		randGen = new Random(seed);
		randSentFrom = new Random(seed2);
		this.probSentFromAgent = _probSentFromAgent; 
	}
	
	public void init(){
		
		try {
			vUserAgents = new Vector<String>();
			agentFile = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(agentFileName), "UTF-8"));
			
			extractAgents(); 
			
			agentFile.close();
			
			System.out.println("Done ... " + vUserAgents.size() + " agents have been extracted ");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void extractAgents(){
		String line; 
		
		try {
			while ((line = agentFile.readLine()) != null){
				vUserAgents.add(line.trim());
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setPostUserAgent(ReducedUserProfile user, Post post){
		// Sent from user's agent
		if (user.isHaveSmartPhone() && (randSentFrom.nextDouble() > probSentFromAgent)){
			post.setUserAgent(getUserAgent(user.getAgentIdx()));
		}
		else
			post.setUserAgent("");
	}

	public void setCommentUserAgent(Friend friend, Comment comment){
		// Sent from user's agent
		if (friend.isHaveSmartPhone() && (randSentFrom.nextDouble() > probSentFromAgent)){
			comment.setUserAgent(getUserAgent(friend.getAgentIdx()));
		}
		else
			comment.setUserAgent("");
	}
	
	public void setPhotoUserAgent(ReducedUserProfile user, Photo photo){
		// Sent from user's agent
		if (user.isHaveSmartPhone() && (randSentFrom.nextDouble() > probSentFromAgent)){
			photo.setUserAgent(getUserAgent(user.getAgentIdx()));
		}
		else
			photo.setUserAgent("");
	}
	
	public String getUniformRandomAgent(){
		int randIdx = randGen.nextInt(vUserAgents.size());
		
		return vUserAgents.get(randIdx); 
	}
	
	public byte getRandomUserAgentIdx(){
		return (byte)randGen.nextInt(vUserAgents.size());
	}	
	public String getUserAgent(int idx){
		return vUserAgents.get(idx);
	}
}
