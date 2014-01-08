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
package ldbc.socialnet.dbgen.objects;

import java.io.Serializable;
import java.util.TreeSet;
import java.util.Iterator;

public class UserProfile implements Serializable {
    private static final long serialVersionUID = 3657773293974543890L;
	int 				accountId;
	int 				locationIdx; 
	int 				locationZId;
	int                 cityIdx; 
	

	int 				randomIdx; 
	int 				locationOrganizationId;
	int 				forumWallId; 
	int 				forumStatusId;
	long	 			createdDate; 
	public short 		numFriends;
	public short 		numTags;

	public short 		numPassFriends[];		// Max number of friends can be 
												// generated after kth passes
	
	public short		lastLocationFriendIdx; 
	public short		startInterestFriendIdx; 
	public short 		lastInterestFriendIdx; 
	
	public short 		numFriendsAdded;
	Friend 				friendList[];
	TreeSet<Integer>	friendIds; 		// Use a Treeset for checking the existence
	
	TreeSet<Integer> 	setOfTags;
	int					mainTagId; 

	//For user's agent information
	boolean				isHaveSmartPhone; 		// Use for providing the user agent information
	byte 				agentIdx; 				// Index of user agent in the dictionary, e.g., 0 for iPhone, 1 for HTC
	byte				browserIdx;				// Index of web browser, e.g., 0 for Internet Explorer
	
	//For IP address
	IP					ipAddress;				// IP address
	
	//For popular places
	short				popularPlaceIds[]; 
	byte				numPopularPlace; 
	
	
	// For dimesion of university
	byte 				gender; 
	long				birthDay;
	
	
	public UserProfile(int accountId) {
	    this.accountId = accountId;
        locationIdx = -1; 
        locationOrganizationId = -1; 
        forumWallId = -1; 
        forumStatusId = -1;
        
        setOfTags = new TreeSet<Integer>();
	}
	
	public byte getGender() {
		return gender;
	}
	public void setGender(byte gender) {
		this.gender = gender;
	}
	public long getBirthDay() {
		return birthDay;
	}
	public void setBirthDay(long birthDay) {
		this.birthDay = birthDay;
	}
	public short getPopularId(int index){
		return popularPlaceIds[index];
	}
	public short[] getPopularPlaceIds() {
		return popularPlaceIds;
	}
	public void setPopularPlaceIds(short[] popularPlaceIds) {
		this.popularPlaceIds = popularPlaceIds;
	}
	public byte getNumPopularPlace() {
		return numPopularPlace;
	}
	public void setNumPopularPlace(byte numPopularPlace) {
		this.numPopularPlace = numPopularPlace;
	}
	public IP getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(IP ipAddress) {
		this.ipAddress = ipAddress;
	}

	public short getNumFriendsAdded() {
		return numFriendsAdded;
	}
	
	public void addNewFriend(Friend friend) {
		friendList[numFriendsAdded] = friend;
		friendIds.add(friend.getFriendAcc());
		numFriendsAdded++;
	}
	
	public boolean isExistFriend(int friendId){
		return friendIds.contains(friendId);
	}
	
	public void setNumFriendsAdded(short numFriendsAdded) {
		this.numFriendsAdded = numFriendsAdded;
	}


	public void print(){
		System.out.println("Account Id: " + accountId);
		System.out.println("User location: " + locationIdx);
		System.out.println("Friends added: " + numFriendsAdded + " / " + numFriends);
		System.out.println("Number of location friends " + numPassFriends[0]);
		System.out.println("Number of interest friends " + numPassFriends[1]);
	}
	public void printDetail(){
		System.out.println("Account Id: " + accountId);
		System.out.println("User location: " + locationIdx);
		System.out.print("Total number of friends: " + numFriends);
		System.out.print(numFriendsAdded + " user friends added: ");
		for (int i = 0; i < numFriendsAdded; i ++){
			System.out.print(" " + friendList[i].getFriendAcc());
		}
		System.out.println();
		System.out.print("User tags: ");
		Iterator<Integer> it = setOfTags.iterator(); 
		while (it.hasNext()){
			System.out.print(" " + it.next()); 
		}
		
		System.out.println();
	}
	public void printTags(){
		System.out.println("Set of tag for " + accountId);
		Iterator<Integer> it = setOfTags.iterator(); 
		while (it.hasNext()){
			System.out.print(" " + it.next()); 
		}
		System.out.println();
	}
	public short getNumPassFriends(int pass) {
		return numPassFriends[pass];
	}
	public void setNumPassFriends(short numPassFriends, int pass) {
		this.numPassFriends[pass] = numPassFriends;
	}
	public TreeSet<Integer> getSetOfTags() {
		return setOfTags;
	}
	public int getFirstTagIdx(){
		Iterator<Integer> iter = setOfTags.iterator();

		int tagIdx = ((Integer)iter.next()).intValue();
		
		return tagIdx;
	}
	public void setSetOfTags(TreeSet<Integer> setOfTags) {
		this.setOfTags = setOfTags;
	}
	
	public void allocateFriendListMemory(int numFriendPasses){
		friendList = new Friend[numFriends];
		friendIds = new TreeSet<Integer>();
		numPassFriends = new short[numFriendPasses];
	}

	public Friend[] getFriendList() {
		return friendList;
	}

	public void setFriendList(Friend[] friendList) {
		this.friendList = friendList;
	}

	public short getNumFriends() {
		return numFriends;
	}

	public void setNumFriends(short numFriends) {
		this.numFriends = numFriends;
	}

	public long getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(long createdDate) {
		this.createdDate = createdDate;
	}
	
	public int getAccountId() {
		return accountId;
	}

	public int getForumWallId() {
		return forumWallId;
	}
	public void setForumWallId(int forumWallId) {
		this.forumWallId = forumWallId;
	}
	public int getForumStatusId() {
		return forumStatusId;
	}
	public void setForumStatusId(int forumStatusId) {
		this.forumStatusId = forumStatusId;
	} 
	
	public int getLocationIdx() {
		return locationIdx;
	}

	public void setLocationIdx(int locationIdx) {
		this.locationIdx = locationIdx;
	}
	
	public int getCityIdx() {
        return cityIdx;
    }

    public void setCityIdx(int cityIdx) {
        this.cityIdx = cityIdx;
    }

	public int getLocationOrganizationId() {
		return locationOrganizationId;
	}

	public void setLocationOrganizationId(int locationOrganizationId) {
		this.locationOrganizationId = locationOrganizationId;
	}
	public short getLastLocationFriendIdx() {
		return lastLocationFriendIdx;
	}
	public void setLastLocationFriendIdx(short lastLocationFriendIdx) {
		this.lastLocationFriendIdx = lastLocationFriendIdx;
	}
	public short getLastInterestFriendIdx() {
		return lastInterestFriendIdx;
	}
	public void setLastInterestFriendIdx(short lastInterestFriendIdx) {
		this.lastInterestFriendIdx = lastInterestFriendIdx;
	}	 
	public boolean isHaveSmartPhone() {
		return isHaveSmartPhone;
	}
	public void setHaveSmartPhone(boolean isHaveSmartPhone) {
		this.isHaveSmartPhone = isHaveSmartPhone;
	}
	public byte getAgentIdx() {
		return agentIdx;
	}
	public void setAgentIdx(byte agentIdx) {
		this.agentIdx = agentIdx;
	}
	public short getStartInterestFriendIdx() {
		return startInterestFriendIdx;
	}
	public void setStartInterestFriendIdx(short startInterestFriendIdx) {
		this.startInterestFriendIdx = startInterestFriendIdx;
	}
	public byte getBrowserIdx() {
		return browserIdx;
	}
	public void setBrowserIdx(byte browserIdx) {
		this.browserIdx = browserIdx;
	}
	public int getRandomIdx() {
		return randomIdx;
	}
	public void setRandomIdx(int randomIdx) {
		this.randomIdx = randomIdx;
	}
	public short[] getNumPassFriends() {
		return numPassFriends;
	}
	public void setNumPassFriends(short[] numPassFriends) {
		this.numPassFriends = numPassFriends;
	}
	public int getLocationZId() {
		return locationZId;
	}
	public void setLocationZId(int locationZId) {
		this.locationZId = locationZId;
	}
	public short getNumTags() {
		return numTags;
	}
	public void setNumTags(short numTags) {
		this.numTags = numTags;
	}
	public int getMainTagId() {
		return mainTagId;
	}
	public void setMainTagId(int mainTagId) {
		this.mainTagId = mainTagId;
	}

}
