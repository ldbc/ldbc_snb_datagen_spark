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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.TreeSet;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public class ReducedUserProfile implements Serializable, Writable{
	private static final long serialVersionUID = 3657773293974543890L;
	private long 				accountId;              /**< @brief The account identifier.*/
	private long	 			creationDate;           /**< @brief User's creation date.*/

    /** Friends and correlation dimensions **/
	private short 		        maxNumFriends;          /**< @brief The maximum number of friends the user can have.**/
	private short 		        numFriends;             /**< @brief The number of friends the user has.**/
	private byte			    numCorDimensions;       /**< @brief The number of correlation dimensions.**/
	private short 		        corMaxNumFriends[];     /**< @brief The number of maximum friends per correlation dimension.**/
    private short 		        corNumFriends[];        /**< @brief The number of friends added per correlation dimension.**/
	private Friend 				friendList[];           /**< @brief The list of friends.**/
	private TreeSet<Long>	    friendIds;              /**< @brief The set of friends' ids.**/
    private int					corIds[];           	/**< @brief The ids of each correlation dimension.**/

	/** For user's agent information **/
	private boolean				isHaveSmartPhone; 		/**< @brief Used to know whether the user has a smartphone or not.**/
	private int 				agentId; 				/**< @brief Index of user agent in the dictionary, e.g., 0 for iPhone, 1 for HTC **/
	private int				browserId;				/**< @brief Index of web browser, e.g., 0 for Internet Explorer **/
	
	/** For IP address **/
	private boolean 			isFrequentChange;		/**< @brief About 1% of users frequently change their location.**/
	private IP					ipAddress;				/**< @brief IP address. **/
	
	
	/** Store redundant info **/
	private int 				countryId;              /**< @brief The home country of the user.**/
	private int                 cityId;                 /**< @brief The city identifier of the country of the user the user lives in. **/
	private long 				wallId;                 /**< @brief The id of the group represeting the wall of the user.**/
	private TreeSet<Integer> 	interests;              /**< @brief The set of interests of the user.**/
    private int                 mainInterest;           /**< @brief The main user interest.*/
	
	private short				popularPlaceIds[];      /**< @brief The set of popular places the user visits.**/
	private byte				numPopularPlace;        /**< @brief The number of popular places of the user.**/
	
	/** For organization dimension **/
	private int 				universityLocationId;   /**< @brief The university location id where the user studied.**/
    private byte				gender;                 /**< @brief The gender of the user.**/
	private long				birthDay;               /**< @brief The birthday of the user.**/

	// For posting
	private boolean 			isLargePoster;          /**< @brief Specifies whether the user is a large poster or not.*/
	
	static public class Counts {
		public int numberOfPosts;
		public int[] numberOfPostsPerMonth;
		public int[] numberOfGroupsPerMonth;
		public int numberOfLikes;
		public int numberOfGroups;
		public int numberOfWorkPlaces;
		public int numberOfTagsOfPosts;
		public int numberOfFriends;
		public int numberOfPostReplies;
		
		public Counts(Counts other){
			this.numberOfPosts = other.numberOfPosts;
			this.numberOfPostsPerMonth = other.numberOfPostsPerMonth;
			this.numberOfLikes = other.numberOfLikes;
			this.numberOfGroups = other.numberOfGroups;
			this.numberOfWorkPlaces = other.numberOfWorkPlaces;
			this.numberOfTagsOfPosts = other.numberOfTagsOfPosts;
			this.numberOfFriends = other.numberOfFriends;
		}
		public Counts(){
			this.numberOfPosts = 0;
			this.numberOfPostsPerMonth = new int[36+1];
			this.numberOfGroupsPerMonth = new int[36+1];
			this.numberOfLikes = 0;
			this.numberOfGroups = 0;
			this.numberOfWorkPlaces = 0;
			this.numberOfTagsOfPosts = 0;
			this.numberOfFriends = 0;
		}
	}

	Counts stats;
	
	public void clear(){
		Arrays.fill(friendList,null);
		friendList = null;
		friendIds.clear();
		friendIds = null;
		corMaxNumFriends = null;
		corNumFriends = null;
		corIds = null;
		interests.clear();
		interests = null;
		popularPlaceIds = null; 
		stats = null;
	}
	
	private void readObject(java.io.ObjectInputStream stream)
			 throws IOException, ClassNotFoundException{
			accountId = stream.readLong();
            //sdpId = stream.readInt();
			creationDate = stream.readLong();
			maxNumFriends = stream.readShort();
			numFriends = stream.readShort();
			numCorDimensions = stream.readByte();
			corMaxNumFriends = new short[numCorDimensions];
			for (int i = 0; i < numCorDimensions; i ++){
				corMaxNumFriends[i] = stream.readShort();
			}
			corNumFriends = new short[numCorDimensions];
			for (int i = 0; i < numCorDimensions; i ++){
				corNumFriends[i] = stream.readShort();
			}
			friendList = new Friend[maxNumFriends];
			friendIds = new TreeSet<Long>();
			for (int i = 0; i < numFriends; i++){
				Friend fr = new Friend(); 
				fr.readFields(stream);
				friendList[i] = fr; 
			}
			//Read the size of Treeset first
			int size = stream.readInt(); 
			for (int i = 0; i < size; i++){
				friendIds.add(stream.readLong());
			}
			corIds = new int[numCorDimensions];
			for (int i = 0; i < numCorDimensions; i++){
				corIds[i] = stream.readInt();
			}
			
			isHaveSmartPhone = stream.readBoolean();
			agentId = stream.readInt();
			browserId = stream.readInt();
			isFrequentChange = stream.readBoolean();

			int ip = stream.readInt();
	        int mask = stream.readInt();
	        ipAddress = new IP(ip, mask); 
			
			countryId = stream.readInt();
			cityId = stream.readInt();
			wallId = stream.readLong();
			//forumStatusId = stream.readInt();
			
			byte numOfTags = stream.readByte();
			interests = new TreeSet<Integer>();
			for (byte i = 0; i < numOfTags;i++){
				interests.add(stream.readInt());
			}
            mainInterest = stream.readInt();
			
			numPopularPlace = stream.readByte(); 
			popularPlaceIds = new short[numPopularPlace];
			for (byte i=0; i < numPopularPlace; i++){
				popularPlaceIds[i] = stream.readShort();
			}
			
			universityLocationId = stream.readInt();
			gender = stream.readByte();
			birthDay = stream.readLong();
			isLargePoster = stream.readBoolean();
			
			stats = new Counts();
			stats.numberOfPosts = stream.readInt();
			stats.numberOfLikes = stream.readInt();
			stats.numberOfGroups = stream.readInt();
			stats.numberOfWorkPlaces = stream.readInt();
			stats.numberOfTagsOfPosts = stream.readInt();
			stats.numberOfFriends = stream.readInt();
	 }
	
	private void writeObject(java.io.ObjectOutputStream stream)
	throws IOException{
		 	stream.writeLong(accountId);
//            stream.writeInt(sdpId);
			stream.writeLong(creationDate);
			stream.writeShort(maxNumFriends);
			stream.writeShort(numFriends);
			stream.writeByte(numCorDimensions);
			for (int i = 0; i < numCorDimensions; i ++){
				stream.writeShort(corMaxNumFriends[i]);
			}
			for (int i = 0; i < numCorDimensions; i ++){
				stream.writeShort(corNumFriends[i]);
			}
			
			for (int i = 0; i < numFriends; i++){
				friendList[i].write(stream);
			}
			//Read the size of Treeset first
			stream.writeInt(friendIds.size()); 
			Iterator<Long> it = friendIds.iterator();
			while (it.hasNext()){
				stream.writeLong(it.next());
			}
			
			for (int i = 0; i < numCorDimensions; i++){
				stream.writeInt(corIds[i]);
			}
			
			stream.writeBoolean(isHaveSmartPhone);
			stream.writeInt(agentId);
			stream.writeInt(browserId);
			stream.writeBoolean(isFrequentChange);
			
			stream.writeInt(ipAddress.getIp());
			stream.writeInt(ipAddress.getMask());

			stream.writeInt(countryId);
			stream.writeInt(cityId);
			stream.writeLong(wallId);

			stream.writeByte((byte)interests.size());
			Iterator<Integer> iter2 = interests.iterator();
			while (iter2.hasNext()){
				stream.writeInt(iter2.next());
			}
            stream.writeInt(mainInterest);

			stream.writeByte(numPopularPlace); 
			for (byte i=0; i < numPopularPlace; i++){
				stream.writeShort(popularPlaceIds[i]);
			}
			
			stream.writeInt(universityLocationId);
			stream.writeByte(gender);
			stream.writeLong(birthDay);
			stream.writeBoolean(isLargePoster);
			
			stream.writeInt(stats.numberOfPosts);
			stream.writeInt(stats.numberOfLikes);
			stream.writeInt(stats.numberOfGroups);
			stream.writeInt(stats.numberOfWorkPlaces);
			stream.writeInt(stats.numberOfTagsOfPosts);
			stream.writeInt(stats.numberOfFriends);
	 }
	
	public void readFields(DataInput arg0) throws IOException {
		accountId = arg0.readLong();
//        sdpId = arg0.readInt();
		creationDate = arg0.readLong();
		maxNumFriends = arg0.readShort();
		numFriends = arg0.readShort();
		numCorDimensions = arg0.readByte();
		corMaxNumFriends = new short[numCorDimensions];
		for (int i = 0; i < numCorDimensions; i ++){
			corMaxNumFriends[i] = arg0.readShort();
		}
		corNumFriends = new short[numCorDimensions];
		for (int i = 0; i < numCorDimensions; i ++){
			corNumFriends[i] = arg0.readShort();
		}
		friendList = new Friend[maxNumFriends];
		friendIds = new TreeSet<Long>();
		for (int i = 0; i < numFriends; i++){
			Friend fr = new Friend(); 
			fr.readFields(arg0);
			friendList[i] = fr; 
		}
		//Read the size of Treeset first
		int size = arg0.readInt(); 
		for (int i = 0; i < size; i++){
			friendIds.add(arg0.readLong());
		}
		corIds = new int[numCorDimensions];
		for (int i = 0; i < numCorDimensions; i++){
			corIds[i] = arg0.readInt();
		}
		
		isHaveSmartPhone = arg0.readBoolean();
		agentId = arg0.readInt();
		browserId = arg0.readInt();
		isFrequentChange = arg0.readBoolean();

		int ip = arg0.readInt();
		int mask = arg0.readInt();
		ipAddress = new IP(ip, mask); 
		
		countryId = arg0.readInt();
		cityId = arg0.readInt();
		wallId = arg0.readLong();
		//forumStatusId = arg0.readInt();
		
		byte numTags = arg0.readByte(); 
		interests = new TreeSet<Integer>();
		for (byte i = 0; i < numTags;i++){
			interests.add(arg0.readInt());
		}
        mainInterest = arg0.readInt();
		numPopularPlace = arg0.readByte();
		popularPlaceIds = new short[numPopularPlace];
		for (byte i=0; i < numPopularPlace; i++){
			popularPlaceIds[i] = arg0.readShort();
		}
		
		universityLocationId = arg0.readInt();
		gender = arg0.readByte();
		birthDay = arg0.readLong();
		isLargePoster = arg0.readBoolean();
		
		stats = new Counts();
		stats.numberOfPosts = arg0.readInt();
		stats.numberOfLikes = arg0.readInt();
		stats.numberOfGroups = arg0.readInt();
		stats.numberOfWorkPlaces = arg0.readInt();
		stats.numberOfTagsOfPosts = arg0.readInt();		
		stats.numberOfFriends = arg0.readInt();
	}
	
	public void copyFields(ReducedUserProfile user){
		accountId = user.accountId;
//        sdpId = user.sdpId;
		creationDate = user.creationDate;
		maxNumFriends = user.maxNumFriends;
		numFriends = user.numFriends;
        numCorDimensions = user.numCorDimensions;
        corMaxNumFriends = user.corMaxNumFriends;
		corNumFriends = user.corNumFriends;
		friendList = user.friendList;
		friendIds = user.friendIds;
		corIds = user.corIds;
		isHaveSmartPhone = user.isHaveSmartPhone;
		agentId = user.agentId;
		browserId = user.browserId;
		isFrequentChange = user.isFrequentChange;
		ipAddress = user.ipAddress;
		countryId = user.countryId;
		cityId = user.cityId;
		wallId = user.wallId;
		interests = user.interests;
        mainInterest = user.mainInterest;
		numPopularPlace = user.numPopularPlace;
		popularPlaceIds = user.popularPlaceIds;
		universityLocationId = user.universityLocationId;
		gender = user.gender;
		birthDay = user.birthDay;
		isLargePoster = user.isLargePoster;
		stats = user.stats;
		stats.numberOfWorkPlaces = user.stats.numberOfWorkPlaces;
	}
	
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(accountId);
//        arg0.writeInt(sdpId);
		arg0.writeLong(creationDate);
		arg0.writeShort(maxNumFriends);
		arg0.writeShort(numFriends);
		arg0.writeByte(numCorDimensions);
		for (int i = 0; i < numCorDimensions; i ++){
			arg0.writeShort(corMaxNumFriends[i]);
		}
		for (int i = 0; i < numCorDimensions; i ++){
			 arg0.writeShort(corNumFriends[i]);
		}
		
		for (int i = 0; i < numFriends; i++){
			friendList[i].write(arg0);
		}
		//Read the size of Treeset first
		arg0.writeInt(friendIds.size()); 
		Iterator<Long> it = friendIds.iterator();
		while (it.hasNext()){
			arg0.writeLong(it.next());
		}
		
		for (int i = 0; i < numCorDimensions; i++){
			arg0.writeInt(corIds[i]);
		}
		
		arg0.writeBoolean(isHaveSmartPhone);
		arg0.writeInt(agentId);
		arg0.writeInt(browserId);
		arg0.writeBoolean(isFrequentChange);

		arg0.writeInt(ipAddress.getIp());
		arg0.writeInt(ipAddress.getMask());
		
		
		arg0.writeInt(countryId);
		arg0.writeInt(cityId);
		arg0.writeLong(wallId);

		arg0.writeByte((byte)interests.size());
		Iterator<Integer> iter2 = interests.iterator();
		while (iter2.hasNext()){
			arg0.writeInt(iter2.next());
		}
        arg0.writeInt(mainInterest);

		
		arg0.writeByte(numPopularPlace); 
		for (byte i=0; i < numPopularPlace; i++){
			arg0.writeShort(popularPlaceIds[i]);
		}
		
		arg0.writeInt(universityLocationId);
		arg0.writeByte(gender);
		arg0.writeLong(birthDay);
		arg0.writeBoolean(isLargePoster);
		
		arg0.writeInt(stats.numberOfPosts);
		arg0.writeInt(stats.numberOfLikes);
		arg0.writeInt(stats.numberOfGroups);
		arg0.writeInt(stats.numberOfWorkPlaces);
		arg0.writeInt(stats.numberOfTagsOfPosts);		
		arg0.writeInt(stats.numberOfFriends);
	}

	public ReducedUserProfile(){
		stats = new Counts();
	}
	
	public int getCorId(int index) {
		return corIds[index];
	}

    public void setMaxNumFriends( short numFriends ) {
        this.maxNumFriends = numFriends;
        this.allocateFriendListMemory();
    }

    public short getMaxNumFriends() {
        return maxNumFriends;
    }

	public void setCorNumFriends(int pass, short numFriends) {
		corNumFriends[pass] = numFriends;
	}

    public short getCorNumFriends(int pass) {
        return corNumFriends[pass];
    }

    public void setCorMaxNumFriends(int pass, short numFriends) {
        corMaxNumFriends[pass] = numFriends;
    }

    public short getCorMaxNumFriends(int pass) {
        return corMaxNumFriends[pass];
    }

	public short getNumFriends() {
		return numFriends;
	}

	public void addNewFriend(Friend friend) {
	    if (friend != null && !friendIds.contains(friend.getFriendAcc())) {
	        friendList[numFriends] = friend;
	        friendIds.add(friend.getFriendAcc());
	        numFriends++;
	    }
	}
	
	public boolean isExistFriend(long friendId){
		return friendIds.contains(friendId);
	}
	

    public void setNumCorDimensions( int numCors )	 {
        this.numCorDimensions = (byte)numCors;
        this.corIds = new int[numCors];
        this.corNumFriends = new short[numCors];
        this.corMaxNumFriends = new short[numCors];
    }
	public void allocateFriendListMemory(){
		friendList = new Friend[maxNumFriends];
		friendIds = new TreeSet<Long>();
	}

	public Friend[] getFriendList() {
		return friendList;
	}
	public void setNumFriends(short numFriends) {
	}
	public long getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}
	public long getAccountId() {
		return accountId;
	}
	public void setAccountId(long accountId) {
		this.accountId = accountId;
	}
	public boolean isHaveSmartPhone() {
		return isHaveSmartPhone;
	}
	public void setHaveSmartPhone(boolean isHaveSmartPhone) {
		this.isHaveSmartPhone = isHaveSmartPhone;
	}
	public int getAgentId() {
		return agentId;
	}
	public void setAgentId(int agentId) {
		this.agentId = agentId;
	}
	public int getBrowserId() {
		return browserId;
	}
	public void setBrowserId(int browserId) {
		this.browserId = browserId;
	}
	public boolean isFrequentChange() {
		return isFrequentChange;
	}
	public void setFrequentChange(boolean isFrequentChange) {
		this.isFrequentChange = isFrequentChange;
	}
	public IP getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(IP ipAddress) {
		this.ipAddress = ipAddress;
	}

	public int getCountryId() {
		return countryId;
	}
	public void setCountryId(int countryId ) {
		this.countryId = countryId;
	}
	public int getCityId() {
        return cityId;
    }
    public void setCityId(int cityId) {
        this.cityId = cityId;
    }
	public long getForumWallId() {
		return wallId;
	}
	public void setForumWallId(long wallId) {
		this.wallId = wallId;
	}
	public TreeSet<Integer> getInterests() {
		return interests;
	}
	public void setInterests(TreeSet<Integer> interests) {
		this.interests = interests;
	}
	public byte getNumPopularPlace() {
		return numPopularPlace;
	}
	public void setNumPopularPlace(byte numPopularPlace) {
		this.numPopularPlace = numPopularPlace;
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
	public TreeSet<Long> getFriendIds() {
		return friendIds;
	}
	public int[] getDicElementIds() {
		return corIds;
	}
	public int getUniversityLocationId() {
		return universityLocationId;
	}
	public void setUniversityLocationId(int universityLocatonId) {
		this.universityLocationId = universityLocatonId;
        GregorianCalendar date = new GregorianCalendar();
        date.setTimeInMillis(birthDay);
        int birthYear = date.get(GregorianCalendar.YEAR);
        int organizationDimension = (int) (universityLocationId | (birthYear << 1) | gender);
        corIds[0] = organizationDimension;
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
	public boolean isLargePoster() {
		return this.isLargePoster;
	}
	public void setLargePoster(boolean isLargePoster) {
		this.isLargePoster = isLargePoster;
	}
	
	public int getNumOfPosts(){
		return stats.numberOfPosts;
	}
	
	public int getNumOfLikes(){
		return stats.numberOfLikes;
	}
	
	public int getNumOfGroups(){
		return stats.numberOfGroups;
	}
	
	public int getNumOfWorkPlaces(){
		return stats.numberOfWorkPlaces;
	}
	
	public int getNumOfTagsOfPosts(){
		return stats.numberOfTagsOfPosts;
	}
	
	public void addNumOfPosts(int num){
		stats.numberOfPosts += num;
	}
	
	public void addNumOfGroups(int num){
		stats.numberOfGroups += num;
	}
	
	public void addNumOfWorkPlaces(int num){
		stats.numberOfWorkPlaces += num;
	}
	
	public void addNumOfTagsOfPosts(int num){
		stats.numberOfTagsOfPosts += num;
	}
	
	public void addNumOfLikesToPosts(int num){
		stats.numberOfLikes += num;
	}
    public void setMainTag( int mainInterest ) {
        this.mainInterest = mainInterest;
        corIds[1] = this.mainInterest;
    }

    public void setRandomId( int randomId ) {
        corIds[2] = randomId;
    }
}
