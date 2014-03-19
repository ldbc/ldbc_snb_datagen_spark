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
	long 				accountId;
    int					sdpId;
	long	 			creationDate;
	public short 		numFriends;
	public short 		numFriendsAdded;
	public byte			numCorDimensions; 
	public short 		numPassFriends[];
	public short 		numPassFriendsAdded[];
	

	Friend 				friendList[];
	TreeSet<Long>	friendIds; 		// Use a Treeset for checking the existence
	

	int					dicElementIds[];	// Id of an element in a dictionary, e.g., locationId
										// interestId
	
	//For user's agent information
	boolean				isHaveSmartPhone; 		// Use for providing the user agent information
	byte 				agentIdx; 				// Index of user agent in the dictionary, e.g., 0 for iPhone, 1 for HTC
	byte				browserIdx;				// Index of web browser, e.g., 0 for Internet Explorer
	
	//For IP address
	boolean 			isFrequentChange;		// About 1% of users frequently change their location
	IP					ipAddress;				// IP address
	
	
	// Store redundant info
	int 				locationId;
	int                 cityIdx;
	int 				forumWallId; 
//	int 				forumStatusId;
	TreeSet<Integer> 	setOfTags;
	
	short				popularPlaceIds[]; 
	byte				numPopularPlace;
	
	// For organization dimension
	int 				universityLocatonId;	
    byte				gender; 
	long				birthDay;

	// For posting
	boolean 			isLargePoster;

	public void clear(){
		Arrays.fill(friendList,null);
		friendList = null;
		friendIds.clear();
		friendIds = null;
		numPassFriends = null; 
		numPassFriendsAdded = null; 
		dicElementIds = null; 
		setOfTags.clear();
		setOfTags = null;
		popularPlaceIds = null; 
	}
	
	private void readObject(java.io.ObjectInputStream stream)
			 throws IOException, ClassNotFoundException{
			// TODO Auto-generated method stub
			accountId = stream.readLong();
            sdpId = stream.readInt();
			creationDate = stream.readLong();
			numFriends = stream.readShort();
			numFriendsAdded = stream.readShort();
			numCorDimensions = stream.readByte();
			numPassFriends = new short[numCorDimensions];
			for (int i = 0; i < numCorDimensions; i ++){
				numPassFriends[i] = stream.readShort();
			}
			numPassFriendsAdded = new short[numCorDimensions];
			for (int i = 0; i < numCorDimensions; i ++){
				numPassFriendsAdded[i] = stream.readShort();
			}
			friendList = new Friend[numFriends];
			friendIds = new TreeSet<Long>();
			for (int i = 0; i < numFriendsAdded; i++){
				Friend fr = new Friend(); 
				fr.readFields(stream);
				friendList[i] = fr; 
			}
			//Read the size of Treeset first
			int size = stream.readInt(); 
			for (int i = 0; i < size; i++){
				friendIds.add(stream.readLong());
			}
			dicElementIds = new int[numCorDimensions];
			for (int i = 0; i < numCorDimensions; i++){
				dicElementIds[i] = stream.readInt();
			}
			
			isHaveSmartPhone = stream.readBoolean();
			agentIdx = stream.readByte();
			browserIdx = stream.readByte();
			isFrequentChange = stream.readBoolean();

			int ip = stream.readInt();
	        int mask = stream.readInt();
	        ipAddress = new IP(ip, mask); 
			
			locationId = stream.readInt();
			cityIdx = stream.readInt();
			forumWallId = stream.readInt();
			//forumStatusId = stream.readInt();
			
			byte numOfTags = stream.readByte();
			setOfTags = new TreeSet<Integer>();
			for (byte i = 0; i < numOfTags;i++){
				setOfTags.add(stream.readInt());
			}
			
			numPopularPlace = stream.readByte(); 
			popularPlaceIds = new short[numPopularPlace];
			for (byte i=0; i < numPopularPlace; i++){
				popularPlaceIds[i] = stream.readShort();
			}
			
			universityLocatonId = stream.readInt(); 
			gender = stream.readByte();
			birthDay = stream.readLong();
			isLargePoster = stream.readBoolean();
	 }
	
	private void writeObject(java.io.ObjectOutputStream stream)
	throws IOException{
		 	stream.writeLong(accountId);
            stream.writeInt(sdpId);
			stream.writeLong(creationDate);
			stream.writeShort(numFriends);
			stream.writeShort(numFriendsAdded);
			stream.writeByte(numCorDimensions);
			for (int i = 0; i < numCorDimensions; i ++){
				stream.writeShort(numPassFriends[i]);
			}
			for (int i = 0; i < numCorDimensions; i ++){
				stream.writeShort(numPassFriendsAdded[i]);
			}
			
			for (int i = 0; i < numFriendsAdded; i++){
				friendList[i].write(stream);
			}
			//Read the size of Treeset first
			stream.writeInt(friendIds.size()); 
			Iterator<Long> it = friendIds.iterator();
			while (it.hasNext()){
				stream.writeLong(it.next());
			}
			
			for (int i = 0; i < numCorDimensions; i++){
				stream.writeInt(dicElementIds[i]);
			}
			
			stream.writeBoolean(isHaveSmartPhone);
			stream.writeByte(agentIdx);
			stream.writeByte(browserIdx);
			stream.writeBoolean(isFrequentChange);
			
			stream.writeInt(ipAddress.getIp());
			stream.writeInt(ipAddress.getMask());
			
			
			stream.writeInt(locationId);
			stream.writeInt(cityIdx);
			stream.writeInt(forumWallId);
//			stream.writeInt(forumStatusId);
			
			stream.writeByte((byte)setOfTags.size());
			Iterator<Integer> iter2 = setOfTags.iterator();
			while (iter2.hasNext()){
				stream.writeInt(iter2.next());
			}

			
			stream.writeByte(numPopularPlace); 
			for (byte i=0; i < numPopularPlace; i++){
				stream.writeShort(popularPlaceIds[i]);
			}
			
			stream.writeInt(universityLocatonId);
			stream.writeByte(gender);
			stream.writeLong(birthDay);
			stream.writeBoolean(isLargePoster);
	 }
	
	public void readFields(DataInput arg0) throws IOException {
		accountId = arg0.readLong();
        sdpId = arg0.readInt();
		creationDate = arg0.readLong();
		numFriends = arg0.readShort();
		numFriendsAdded = arg0.readShort();
		numCorDimensions = arg0.readByte();
		numPassFriends = new short[numCorDimensions];
		for (int i = 0; i < numCorDimensions; i ++){
			numPassFriends[i] = arg0.readShort();
		}
		numPassFriendsAdded = new short[numCorDimensions];
		for (int i = 0; i < numCorDimensions; i ++){
			numPassFriendsAdded[i] = arg0.readShort();
		}
		friendList = new Friend[numFriends];
		friendIds = new TreeSet<Long>();
		for (int i = 0; i < numFriendsAdded; i++){
			Friend fr = new Friend(); 
			fr.readFields(arg0);
			friendList[i] = fr; 
		}
		//Read the size of Treeset first
		int size = arg0.readInt(); 
		for (int i = 0; i < size; i++){
			friendIds.add(arg0.readLong());
		}
		dicElementIds = new int[numCorDimensions];
		for (int i = 0; i < numCorDimensions; i++){
			dicElementIds[i] = arg0.readInt();
		}
		
		isHaveSmartPhone = arg0.readBoolean();
		agentIdx = arg0.readByte();
		browserIdx = arg0.readByte();
		isFrequentChange = arg0.readBoolean();

		int ip = arg0.readInt();
		int mask = arg0.readInt();
		ipAddress = new IP(ip, mask); 
		
		locationId = arg0.readInt();
		cityIdx = arg0.readInt();
		forumWallId = arg0.readInt();
		//forumStatusId = arg0.readInt();
		
		byte numTags = arg0.readByte(); 
		setOfTags = new TreeSet<Integer>();
		for (byte i = 0; i < numTags;i++){
			setOfTags.add(arg0.readInt());
		}
		
		
		numPopularPlace = arg0.readByte(); 
		popularPlaceIds = new short[numPopularPlace];
		for (byte i=0; i < numPopularPlace; i++){
			popularPlaceIds[i] = arg0.readShort();
		}
		
		universityLocatonId = arg0.readInt(); 
		gender = arg0.readByte();
		birthDay = arg0.readLong();
		isLargePoster = arg0.readBoolean();
	}
	
	public void copyFields(ReducedUserProfile user){
		accountId = user.getAccountId();
        sdpId = user.getSdpId();
		creationDate = user.getCreationDate();
		numFriends = user.getNumFriends();
		numFriendsAdded = user.getNumFriendsAdded();
		numCorDimensions = user.getNumCorDimensions();

		numPassFriends = user.getNumPassFriends();
		numPassFriendsAdded = user.getNumPassFriendsAdded();
	
		friendList = user.getFriendList();
		friendIds = user.getFriendIds();
		
		dicElementIds = user.getDicElementIds();
		
		isHaveSmartPhone = user.isHaveSmartPhone();
		agentIdx = user.getAgentIdx();
		browserIdx = user.getBrowserIdx();
		isFrequentChange = user.isFrequentChange;

		ipAddress = user.getIpAddress();  
		
		locationId = user.getLocationId();
		cityIdx = user.getCityId();
		forumWallId = user.getForumWallId();
//		forumStatusId = user.getForumStatusId();
		
		setOfTags = user.getSetOfTags();

		numPopularPlace = user.getNumPopularPlace();
		popularPlaceIds = user.getPopularPlaceIds();
		
		universityLocatonId = user.getUniversityLocationId(); 
		gender = user.getGender();
		birthDay = user.getBirthDay();
		isLargePoster = user.isLargePoster();
	}
	
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(accountId);
        arg0.writeInt(sdpId);
		arg0.writeLong(creationDate);
		arg0.writeShort(numFriends);
		arg0.writeShort(numFriendsAdded);
		arg0.writeByte(numCorDimensions);
		for (int i = 0; i < numCorDimensions; i ++){
			arg0.writeShort(numPassFriends[i]);
		}
		for (int i = 0; i < numCorDimensions; i ++){
			 arg0.writeShort(numPassFriendsAdded[i]);
		}
		
		for (int i = 0; i < numFriendsAdded; i++){
			friendList[i].write(arg0);
		}
		//Read the size of Treeset first
		arg0.writeInt(friendIds.size()); 
		Iterator<Long> it = friendIds.iterator();
		while (it.hasNext()){
			arg0.writeLong(it.next());
		}
		
		for (int i = 0; i < numCorDimensions; i++){
			arg0.writeInt(dicElementIds[i]);
		}
		
		arg0.writeBoolean(isHaveSmartPhone);
		arg0.writeByte(agentIdx);
		arg0.writeByte(browserIdx);
		arg0.writeBoolean(isFrequentChange);

		arg0.writeInt(ipAddress.getIp());
		arg0.writeInt(ipAddress.getMask());
		
		
		arg0.writeInt(locationId);
		arg0.writeInt(cityIdx);
		arg0.writeInt(forumWallId);
//		arg0.writeInt(forumStatusId);
		
		arg0.writeByte((byte)setOfTags.size()); 
		
		Iterator<Integer> iter2 = setOfTags.iterator();
		while (iter2.hasNext()){
			arg0.writeInt(iter2.next());
		}

		
		arg0.writeByte(numPopularPlace); 
		for (byte i=0; i < numPopularPlace; i++){
			arg0.writeShort(popularPlaceIds[i]);
		}
		
		arg0.writeInt(universityLocatonId);
		arg0.writeByte(gender);
		arg0.writeLong(birthDay);
		arg0.writeBoolean(isLargePoster);
	}

	public ReducedUserProfile(){
		
	}
	
	public ReducedUserProfile(UserProfile user, int numCorrDimensions){
		this.setAccountId(user.getAccountId());
        this.setSdpId(user.getSdpId());
		this.setCreationDate(user.getCreationDate());
		this.setNumFriends(user.getNumFriends());
		this.setNumFriendsAdded((short)0);
		this.numCorDimensions = (byte)numCorrDimensions;
		
		this.setUniversityLocationId(user.getUniversityLocationId());
		
		dicElementIds = new int[numCorrDimensions];
		
		this.setGender(user.getGender());
		this.setBirthDay(user.getBirthDay());
		
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(birthDay);
		int birthYear = date.get(GregorianCalendar.YEAR);
		
		int organizationDimension = universityLocatonId | (birthYear << 1) | gender;		
		
		this.setDicElementId(organizationDimension,0);
		
		this.setDicElementId(user.getMainTagId(), 1);
		
		this.setDicElementId(user.getRandomIdx(),2);
		
		this.allocateFriendListMemory();
		
		// for user's agent information
		this.setHaveSmartPhone(user.isHaveSmartPhone());
		this.setAgentIdx(user.getAgentId());
		this.setBrowserIdx(user.getBrowserId());
		this.setIpAddress(user.getIpAddress());
		
		this.setNumPassFriends(user.getNumPassFriends());
		
		// DucPM: Need to check whether this info needs to be stored here
		this.setLocationIdx(user.getLocationId());
		this.setCityIdx(user.getCityId());
		//this.setForumStatusId(user.getForumStatusId());
		this.setForumWallId(user.getForumWallId());
		this.setSetOfTags(user.getSetOfTags());
		this.setPopularPlaceIds(user.getPopularPlaceIds());
		this.setNumPopularPlace(user.getNumPopularPlace());
		
		this.numPassFriendsAdded = new short[numCorrDimensions];
		this.isLargePoster = user.isLargePoster();
	}
	
	public int getDicElementId(int index) {
		return dicElementIds[index];
	}

	public void setDicElementId(int dicElementId, int index) {
		this.dicElementIds[index] = dicElementId;
	}

	public void setPassFriendsAdded(int pass, short numPassFriendAdded) {
		numPassFriendsAdded[pass] = numPassFriendAdded;
	}
	public short getPassFriendsAdded(int pass) {
		return numPassFriendsAdded[pass];
	}	
	
	
	public short getLastInterestFriendIdx(){
		return (short)(numPassFriendsAdded[1] - 1);
	}
	public short getStartInterestFriendIdx(){
		return (short)(numPassFriendsAdded[0]);
	}
	public short getLastLocationFriendIdx(){
		return (short)(numPassFriendsAdded[0] - 1);
	}
	
	public short getNumFriendsAdded() {
		return numFriendsAdded;
	}
	
	public void resetUser(){
		accountId = -1;
		numFriends = 0; 
		numFriendsAdded = 0;
		 
	}
	
	public void addNewFriend(Friend friend) {
	    if (friend != null && !friendIds.contains(friend.getFriendAcc())) {
	        friendList[numFriendsAdded] = friend;
	        friendIds.add(friend.getFriendAcc());
	        numFriendsAdded++;
	    }
	}
	
	public boolean isExistFriend(long friendId){
		return friendIds.contains(friendId);
	}
	
	public void setNumFriendsAdded(short numFriendsAdded) {
		this.numFriendsAdded = numFriendsAdded;
	}
	
	
	public void allocateFriendListMemory(){
		friendList = new Friend[numFriends];
		friendIds = new TreeSet<Long>();
	}

	public Friend[] getFriendList() {
		return friendList;
	}

	public void setFriendList(Friend[] friendList) {
		this.friendList = friendList;
	}
	
	public short getNumFriends(int pass) {
		return numPassFriends[pass];
	}
	public void setNumFriends(short numFriends) {
		this.numFriends = numFriends;
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

	public int getSdpId() {
		return sdpId;
	}

	public void setSdpId(int sdpId) {
		this.sdpId = sdpId;
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

	public byte getBrowserIdx() {
		return browserIdx;
	}

	public void setBrowserIdx(byte browserIdx) {
		this.browserIdx = browserIdx;
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
	public short[] getNumPassFriends() {
		return numPassFriends;
	}
	public void setNumPassFriends(short[] numPassFriends) {
		this.numPassFriends = numPassFriends;
	}
	public int getLocationId() {
		return locationId;
	}
	public void setLocationIdx(int locationId) {
		this.locationId = locationId;
	}
	public int getCityId() {
        return cityIdx;
    }
    public void setCityIdx(int cityIdx) {
        this.cityIdx = cityIdx;
    }
	public int getForumWallId() {
		return forumWallId;
	}
	public void setForumWallId(int forumWallId) {
		this.forumWallId = forumWallId;
	}

	public TreeSet<Integer> getSetOfTags() {
		return setOfTags;
	}

	public void setSetOfTags(TreeSet<Integer> setOfTags) {
		this.setOfTags = setOfTags;
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

	public short getNumFriends() {
		return numFriends;
	}
	public byte getNumCorDimensions() {
		return numCorDimensions;
	}

	public void setNumCorDimensions(byte numCorDimensions) {
		this.numCorDimensions = numCorDimensions;
	}
	public short[] getNumPassFriendsAdded() {
		return numPassFriendsAdded;
	}

	public void setNumPassFriendsAdded(short[] numPassFriendsAdded) {
		this.numPassFriendsAdded = numPassFriendsAdded;
	}
	public TreeSet<Long> getFriendIds() {
		return friendIds;
	}
	public int[] getDicElementIds() {
		return dicElementIds;
	}
	public int getUniversityLocationId() {
		return universityLocatonId;
	}

	public void setUniversityLocationId(int universityLocatonId) {
		this.universityLocatonId = universityLocatonId;
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
}
