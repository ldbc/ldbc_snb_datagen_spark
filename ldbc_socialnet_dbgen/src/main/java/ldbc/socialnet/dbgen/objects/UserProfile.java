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
	private long 				accountId;              /**< @brief A unique account id identifying the user.*/
    private int					sdpId; 		            /**< @brief Social degree percentile Id: computed from percentile in the social degree distribution*/
	private int 				locationId;             /**< @brief The location id of the place were the user lives.*/
	private int 				locationZId;            /**< @brief The location Z id.*/
	private int                 cityId;                 /**< @brief The index of the city where the user lives.*/
	private int 				randomIdx;              /**< @brief Random index used to sort the users in the last map/reduce job*/
	private int 				universityLocationId;   /**< @brief The location identifier of the university where the user studies.*/
	private long 				forumWallId;            /**< @brief The identifier of the wall (group) of the user.*/
	private long	 			creationDate;           /**< @brief The date when the user was created.*/
	private short         		numFriends;             /**< @brief The expected number of friends of the user.*/
	private short 		        numPassFriends[];		/**< @brief Max number of friends can be generated after kth passes*/

	
	private short 		        numFriendsAdded;        /**< @brief The actual number of friends of the user.*/
	private Friend 				friendList[];           /**< @brief The list of friends of the user.*/ 
	private TreeSet<Long>   	friendIds; 		        /**< @brief A set containing the friend identifiers for fast lookup.*/	
	private TreeSet<Integer> 	setOfTags;              /**< @brief The set of tags the user is interested in.*/
	private int					mainTagId;              /**< @brief The principal user interest.*/

	//For user's agent information
	private boolean				isHaveSmartPhone; 		/**< @brief True if the user has a smartphone.*/
    private byte 				agentId; 				/**< @brief Index of user agent in the dictionary, e.g., 0 for iPhone, 1 for HTC*/
	private byte				browserId;				/**< @brief Index of web browser, e.g., 0 for Internet Explorer.*/
	
	//For IP address
	IP					        ipAddress;  	        /**< @brief The IP address from which the account of the user was created.*/
	
	//For popular places
	private short				popularPlaceIds[];      
//	private byte				numPopularPlace; 
	
	
	// For dimesion of university
	private byte 				gender;                 /**< @brief The gender of the user.*/
	private long				birthDay;               /**< @brief The birthDay of the user.*/

	// For posting
	private boolean				isLargePoster;          /**< @brief True if the user is selected as a candidate to post large posts.*/

	private short		        lastLocationFriendIdx; 
	private short		        startInterestFriendIdx; 
	private short 		        lastInterestFriendIdx; 
	
	
	public UserProfile( long accountId,
                        long creationDate,
                        byte gender,
                        long birthDay,
                        byte browserId,
                        int locationId,
                        int locationZId,
                        int cityId,
                        IP ipAddress
                        ) {

	    this.accountId = accountId;
        this.creationDate = creationDate;
        this.gender = gender;
        this.birthDay = birthDay;
        this.browserId = browserId;
        this.locationId = locationId;
        this.locationZId = locationZId;
        this.cityId = cityId;
        this.ipAddress = ipAddress;
        this.forumWallId = -1;
        this.universityLocationId = -1; 
        this.numFriends = 0;
        this.numFriendsAdded = 0;
        this.isLargePoster = false;
        this.setOfTags = new TreeSet<Integer>();
        this.popularPlaceIds = null;
        this.friendList = null;
        this.friendIds = null;
        this.numPassFriends = null;
	}
	public int getSdpId() {
		return sdpId;
	}

	public void setSdpId(int sdpId) {
		this.sdpId = sdpId;
	}
	public byte getGender() {
		return gender;
	}

	public long getBirthDay() {
		return birthDay;
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
		return (byte)(popularPlaceIds.length);
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
		System.out.println("User location: " + locationId);
		System.out.println("Friends added: " + numFriendsAdded + " / " + numFriends);
		System.out.println("Number of location friends " + numPassFriends[0]);
		System.out.println("Number of interest friends " + numPassFriends[1]);
	}
	public void printDetail(){
		System.out.println("Account Id: " + accountId);
		System.out.println("User location: " + locationId);
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
		friendIds = new TreeSet<Long>();
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

	public long getCreationDate() {
		return creationDate;
	}

	public long getAccountId() {
		return accountId;
	}

	public long getForumWallId() {
		return forumWallId;
	}
	
	public int getLocationId() {
		return locationId;
	}

	
	public int getCityId() {
        return cityId;
    }

	public int getUniversityLocationId() {
		return universityLocationId;
	}

	public void setUniversityLocationId(int universityLocationId) {
		this.universityLocationId = universityLocationId;
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
	public byte getAgentId() {
		return agentId;
	}
	public void setAgentId(byte agentId) {
		this.agentId = agentId;
	}
	public short getStartInterestFriendIdx() {
		return startInterestFriendIdx;
	}
	public void setStartInterestFriendIdx(short startInterestFriendIdx) {
		this.startInterestFriendIdx = startInterestFriendIdx;
	}
	public byte getBrowserId() {
		return browserId;
	}
	public void setBrowserId(byte browserId) {
		this.browserId = browserId;
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

	public int getMainTagId() {
		return mainTagId;
	}
	public void setMainTagId(int mainTagId) {
		this.mainTagId = mainTagId;
	}

	public boolean isLargePoster() {
		return this.isLargePoster;
	}

	public void setLargePoster(boolean isLargePoster) {
		this.isLargePoster = isLargePoster;
	}
}
