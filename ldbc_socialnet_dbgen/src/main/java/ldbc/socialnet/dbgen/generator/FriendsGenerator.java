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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.GregorianCalendar;
import java.util.Random;

import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;

import org.apache.hadoop.mapreduce.Reducer.Context;


public class FriendsGenerator {
	/*
	ReducedUserProfile  	reducedUserProfiles[];
	ReducedUserProfile  	cellReducedUserProfiles[];
	
	int						numUserProfilesRead = 0;
	int						numtotalUser; 

	Random 					randUniform;
	private double 			baseExponentialRate = 0.8;
	private double	 		limitProCorrelated = 0.3; 
	double 					baseProbCorrelated = 0.8;
	
	double 				alpha = 2;
	
	int						numUserForNewCell = 0; 
	int 					curCellPos;
	
	int 					lastMapCellPos;
	int 					lastMapCell;
	int 					startMapUserIdx;

	int						cellSize; 
	int						windowSize; 
	int 					numberOfCellPerWindow;
	
	DateGenerator 		dateTimeGenerator;
	int					startYear; 
	int					startMonth; 
	int					startDate;
	int					endYear; 
	int					endMonth;
	int					endDate;
	
	String 			sibOutputDir;
	String 			sibHomeDir;
	String 			paramFileName = "params.ini";
	
	int				reduceIdx;
	
	MRWriter			mrWriter; 
	
	public FriendsGenerator(int _reduceIdx, String _sibOutputDir, String _sibHomeDir){
		reduceIdx = _reduceIdx;
		sibOutputDir = _sibOutputDir;
		sibHomeDir = _sibHomeDir;
		
		System.out.println("Current directory in FriendsGenerator is " + _sibHomeDir);
		
		loadParamsFromFile();
		init(reduceIdx);
	}

	
	public void pushUserProfile(ReducedUserProfile reduceUser, int pass, Context context, int reduceIdx){
		numUserProfilesRead++;
		if (numUserProfilesRead < windowSize){
			reducedUserProfiles[numUserProfilesRead-1] = reduceUser;
		}
		else if (numUserProfilesRead == windowSize){
			reducedUserProfiles[numUserProfilesRead-1] = reduceUser;
			mrInitFriendShipWindow(pass, context, reduceIdx);
		}
		else{
			numUserForNewCell++;
			cellReducedUserProfiles[numUserForNewCell-1] = reduceUser;
			if (numUserForNewCell == cellSize){
				curCellPos++;
				mrSlideFriendShipWindow(pass,curCellPos, context, reduceIdx);
				// Call the split function
			}
		}
		
	}
	
	public void mrGenerateFriendShip(int pass, Context context, int reduceIdx){
		
		
		int curCellPos = 0;
		
		while (curCellPos < lastMapCellPos){
			curCellPos++;
			mrSlideFriendShipWindow(pass,curCellPos, context, reduceIdx);
		}
		
		int numLeftCell = numberOfCellPerWindow - 1;
		while (numLeftCell > 0){
			curCellPos++;
			mrSlideLastCellsFriendShip(pass, curCellPos,	numLeftCell, context);
			numLeftCell--;
		}
		
		System.out.println("Pass "+ pass +" Total " + friendshipNo + " friendships generated");
	}
	
	public void mrInitFriendShipWindow(int pass, Context context, int mapIdx){		//Generate the friendship in the first window
		// Create the friend based on the location info
		//Runtime.getRuntime().gc();

		double randProb;
		
		for (int i = 0; i < cellSize; i++) {
			// From this user, check all the user in the window to create friendship
			for (int j = i + 1; j < windowSize - 1; j++) {
				if (reducedUserProfiles[i].getNumFriendsAdded() 
						== reducedUserProfiles[i].getNumFriends(pass))
					break;
				if (reducedUserProfiles[j].getNumFriendsAdded() 
						== reducedUserProfiles[j].getNumFriends(pass))
					continue;

                if (reducedUserProfiles[i].isExistFriend(
                		reducedUserProfiles[j].getAccountId()))
                    continue;

				// Generate a random value
				randProb = randUniform.nextDouble();
				
				double prob = baseProbCorrelated * Math.pow(baseExponentialRate, 
				Math.abs(reducedUserProfiles[i].getDicElementId(pass)- reducedUserProfiles[j].getDicElementId(pass)));
				
				if ((randProb < prob) || (randProb < limitProCorrelated)) {
					// add a friendship
					createFriendShip(reducedUserProfiles[i], reducedUserProfiles[j],
							(byte) pass);
				}

			}
		}

		updateLastPassFriendAdded(0, cellSize, pass);

		mrWriter.writeReducedUserProfiles(0, cellSize, pass, reducedUserProfiles, context);
	}
	
	public void mrSlideFriendShipWindow(int pass, int cellPos, Context context, int mapIdx){

		// In window, position of new cell = the position of last removed cell =
		// cellPos - 1
		int newCellPosInWindow = (cellPos - 1) % numberOfCellPerWindow;

		int newStartIndex = newCellPosInWindow * cellSize;
		
		// Real userIndex in the social graph
		int newUserIndex = (cellPos + numberOfCellPerWindow - 1) * cellSize + startMapUserIdx;

		int curIdxInWindow;

		// Init the number of friends for each user in the new cell
		generateCellOfUsers(newStartIndex, newUserIndex);

		// Create the friendships
		// Start from each user in the first cell of the window --> at the
		// cellPos, not from the new cell
		newStartIndex = (cellPos % numberOfCellPerWindow) * cellSize;
		for (int i = 0; i < cellSize; i++) {
			curIdxInWindow = newStartIndex + i;
			// Generate set of friends list

			// Here assume that all the users in the window including the new
			// cell have the number of friends
			// and also the number of friends to add

			double randProb;

			if (reducedUserProfiles[curIdxInWindow].getNumFriendsAdded() 
					== reducedUserProfiles[curIdxInWindow].getNumFriends(pass))
				continue;

			// From this user, check all the user in the window to create
			// friendship
			for (int j = i + 1; (j < windowSize - 1)
					&& reducedUserProfiles[curIdxInWindow].getNumFriendsAdded() 
					< reducedUserProfiles[curIdxInWindow].getNumFriends(pass); j++) {

				int checkFriendIdx = (curIdxInWindow + j) % windowSize;

				if (reducedUserProfiles[checkFriendIdx].getNumFriendsAdded() 
						== reducedUserProfiles[checkFriendIdx].getNumFriends(pass))
					continue;

                if (reducedUserProfiles[curIdxInWindow].isExistFriend(
                		reducedUserProfiles[checkFriendIdx].getAccountId()))
                    continue;
                
                
                // Generate a random value
				randProb = randUniform.nextDouble();
				
				double prob = baseProbCorrelated * Math.pow(baseExponentialRate, 
						Math.abs(reducedUserProfiles[curIdxInWindow].getDicElementId(pass)
								- reducedUserProfiles[checkFriendIdx].getDicElementId(pass)));
						
				if ((randProb < prob) || (randProb < limitProCorrelated)) {
					// add a friendship
					createFriendShip(reducedUserProfiles[curIdxInWindow], reducedUserProfiles[checkFriendIdx],
							(byte) pass);
				}
			}

		}

		updateLastPassFriendAdded(newStartIndex, newStartIndex + cellSize, pass);
		mrWriter.writeReducedUserProfiles(newStartIndex, newStartIndex + cellSize, pass, reducedUserProfiles, context);

	}
	
	public void loadParamsFromFile() {
		try {
			RandomAccessFile paramFile;
			paramFile = new RandomAccessFile(sibHomeDir + paramFileName, "r");
			String line;
			while ((line = paramFile.readLine()) != null) {
				String infos[] = line.split(": ");
				if (infos[0].startsWith("numtotalUser")) {
					numtotalUser = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("startYear")) {
					startYear = Integer.parseInt(infos[1].trim());
					continue;	
				} else if (infos[0].startsWith("startMonth")) {
					startMonth = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("startDate")) {
					startDate = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("endYear")) {
					endYear = Integer.parseInt(infos[1].trim());
					continue;	
				} else if (infos[0].startsWith("endMonth")) {
					endMonth = Integer.parseInt(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("endDate")) {
					endDate = Integer.parseInt(infos[1].trim());
					continue;					
				} else if (infos[0].startsWith("cellSize")) {
					cellSize = Short.parseShort(infos[1].trim());
					continue;
				} else if (infos[0].startsWith("numberOfCellPerWindow")) {
					numberOfCellPerWindow = Integer.parseInt(infos[1].trim());
					continue;
				} 
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void init(int reduceIdx){
		windowSize = (int) cellSize * numberOfCellPerWindow;
		
		numUserProfilesRead = 0; 
		
		reducedUserProfiles = new ReducedUserProfile[windowSize];
		cellReducedUserProfiles = new ReducedUserProfile[cellSize];
		
		randUniform = new Random(53223436L + 123123*reduceIdx);
		
		dateTimeGenerator = new DateGenerator(new GregorianCalendar(startYear, startMonth,
				startDate), new GregorianCalendar(endYear, endMonth, endDate), 53223436L, 53221234L,
				alpha);		

		mrWriter = new MRWriter(cellSize, windowSize, sibOutputDir); 
		
		curCellPos = 0; 
	}
	public void updateLastPassFriendAdded(int from, int to, int pass) {
		if (to > windowSize) {
			for (int i = from; i < windowSize; i++) {
				reducedUserProfiles[i].setPassFriendsAdded(pass, reducedUserProfiles[i].getNumFriendsAdded());
			}
			for (int i = 0; i < to - windowSize; i++) {
				reducedUserProfiles[i].setPassFriendsAdded(pass, reducedUserProfiles[i].getNumFriendsAdded());
			}
		} else {
			for (int i = from; i < to; i++) {
				reducedUserProfiles[i].setPassFriendsAdded(pass, reducedUserProfiles[i].getNumFriendsAdded());
			}
		}
	}

	public void createFriendShip(ReducedUserProfile user1, ReducedUserProfile user2, byte pass) {
		long requestedTime = dateTimeGenerator.randomFriendRequestedDate(user1,
				user2);
		byte initiator = (byte) randInitiator.nextInt(2);
		long createdTime = -1;
		long declinedTime = -1;
		if (randFriendReject.nextDouble() > friendRejectRatio) {
			createdTime = dateTimeGenerator
					.randomFriendApprovedDate(requestedTime);
		} else {
			declinedTime = dateTimeGenerator
					.randomFriendDeclinedDate(requestedTime);
			if (randFriendReapprov.nextDouble() < friendReApproveRatio) {
				createdTime = dateTimeGenerator
						.randomFriendReapprovedDate(declinedTime);
			}
		}

		// user2.addNewFriend(new Friend(user1.getAccountId(),requestedTime,
		// declinedTime,createdTime,pass,initiator) );
		// user1.addNewFriend(new Friend(user2.getAccountId(),requestedTime,
		// declinedTime,createdTime,pass,initiator) );

		user2.addNewFriend(new Friend(user1, requestedTime, declinedTime,
				createdTime, pass, initiator));
		user1.addNewFriend(new Friend(user2, requestedTime, declinedTime,
				createdTime, pass, initiator));

		friendshipNo++;
	}
	*/
}
