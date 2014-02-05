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
	int 				accountId;
	long	 			createdDate; 
	public short 		numFriends;


	public short 		numFriendsAdded;
	public byte			numCorDimensions; 
	public short 		numPassFriends[];
	public short 		numPassFriendsAdded[];
	

	Friend 				friendList[];
	TreeSet<Integer>	friendIds; 		// Use a Treeset for checking the existence
	

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
	int 				locationIdx;
	int                 cityIdx;
	int 				forumWallId; 
	int 				forumStatusId;
	TreeSet<Integer> 	setOfTags;
	
	short				popularPlaceIds[]; 
	byte				numPopularPlace;
	
	// For organization dimension
	int 				locationOrganizationIdx;	// Contain zOderLocation  ... OrganizationId
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
			accountId = stream.readInt();
			createdDate = stream.readLong(); 
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
			friendIds = new TreeSet<Integer>();
			for (int i = 0; i < numFriendsAdded; i++){
				Friend fr = new Friend(); 
				fr.readFields(stream);
				friendList[i] = fr; 
			}
			//Read the size of Treeset first
			int size = stream.readInt(); 
			for (int i = 0; i < size; i++){
				friendIds.add(stream.readInt());
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
			
			locationIdx = stream.readInt();
			cityIdx = stream.readInt();
			forumWallId = stream.readInt();
			forumStatusId = stream.readInt();
			
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
			
			locationOrganizationIdx = stream.readInt(); 
			gender = stream.readByte();
			birthDay = stream.readLong();
			isLargePoster = stream.readBoolean();
	 }
	
	private void writeObject(java.io.ObjectOutputStream stream)
	throws IOException{
		 	stream.writeInt(accountId);
			stream.writeLong(createdDate); 
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
			Iterator<Integer> it = friendIds.iterator();
			while (it.hasNext()){
				stream.writeInt(it.next());
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
			
			
			stream.writeInt(locationIdx);
			stream.writeInt(cityIdx);
			stream.writeInt(forumWallId);
			stream.writeInt(forumStatusId);
			
			stream.writeByte((byte)setOfTags.size());
			Iterator<Integer> iter2 = setOfTags.iterator();
			while (iter2.hasNext()){
				stream.writeInt(iter2.next());
			}

			
			stream.writeByte(numPopularPlace); 
			for (byte i=0; i < numPopularPlace; i++){
				stream.writeShort(popularPlaceIds[i]);
			}
			
			stream.writeInt(locationOrganizationIdx);
			stream.writeByte(gender);
			stream.writeLong(birthDay);
			stream.writeBoolean(isLargePoster);
	 }
	
	public void readFields(DataInput arg0) throws IOException {
		accountId = arg0.readInt();
		createdDate = arg0.readLong(); 
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
		friendIds = new TreeSet<Integer>();
		for (int i = 0; i < numFriendsAdded; i++){
			Friend fr = new Friend(); 
			fr.readFields(arg0);
			friendList[i] = fr; 
		}
		//Read the size of Treeset first
		int size = arg0.readInt(); 
		for (int i = 0; i < size; i++){
			friendIds.add(arg0.readInt());
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
		
		locationIdx = arg0.readInt();
		cityIdx = arg0.readInt();
		forumWallId = arg0.readInt();
		forumStatusId = arg0.readInt();
		
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
		
		locationOrganizationIdx = arg0.readInt(); 
		gender = arg0.readByte();
		birthDay = arg0.readLong();
		isLargePoster = arg0.readBoolean();
	}
	
	public void copyFields(ReducedUserProfile user){
		// TODO Auto-generated method stub
		accountId = user.getAccountId();
		createdDate = user.getCreatedDate();
		numFriends = user.getNumFriends();
		numFriendsAdded = user.getNumFriendsAdded();
		numCorDimensions = user.getNumCorDimensions();

		numPassFriends = user.getNumPassFriends();
		numPassFriendsAdded = user.getNumPassFriendsAdded();
	
		friendList = user.getFriendList();
		friendIds = user.getFriendIds();
		
		dicElementIds = user.getDicElementIds();
		
		isHaveSmartPhone = user.isHaveSmartPhone ;
		agentIdx = user.getAgentIdx();
		browserIdx = user.getBrowserIdx();
		isFrequentChange = user.isFrequentChange;

		ipAddress = user.getIpAddress();  
		
		locationIdx = user.getLocationIdx();
		cityIdx = user.getCityIdx();
		forumWallId = user.getForumWallId();
		forumStatusId = user.getForumStatusId();
		
		setOfTags = user.getSetOfTags();

		numPopularPlace = user.getNumPopularPlace();
		popularPlaceIds = user.getPopularPlaceIds();
		
		locationOrganizationIdx = user.getLocationOrganizationIdx(); 
		gender = user.getGender();
		birthDay = user.getBirthDay();
		isLargePoster = user.isLargePoster();
	}
	
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(accountId);
		arg0.writeLong(createdDate); 
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
		Iterator<Integer> it = friendIds.iterator();
		while (it.hasNext()){
			arg0.writeInt(it.next());
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
		
		
		arg0.writeInt(locationIdx);
		arg0.writeInt(cityIdx);
		arg0.writeInt(forumWallId);
		arg0.writeInt(forumStatusId);
		
		arg0.writeByte((byte)setOfTags.size()); 
		
		Iterator<Integer> iter2 = setOfTags.iterator();
		while (iter2.hasNext()){
			arg0.writeInt(iter2.next());
		}

		
		arg0.writeByte(numPopularPlace); 
		for (byte i=0; i < numPopularPlace; i++){
			arg0.writeShort(popularPlaceIds[i]);
		}
		
		arg0.writeInt(locationOrganizationIdx);
		arg0.writeByte(gender);
		arg0.writeLong(birthDay);
		arg0.writeBoolean(isLargePoster);
	}

	public ReducedUserProfile(){
		
	}
	
	public ReducedUserProfile(UserProfile user, int numCorrDimensions){
		this.setAccountId(user.getAccountId());
		this.setCreatedDate(user.getCreatedDate());
		this.setNumFriends(user.getNumFriends());
		this.setNumFriendsAdded((short)0);
		this.numCorDimensions = (byte)numCorrDimensions;
		
		this.setLocationOrganizationIdx(user.getLocationOrganizationId());
		
		dicElementIds = new int[numCorrDimensions];
		
		this.setGender(user.getGender());
		this.setBirthDay(user.getBirthDay());
		
		GregorianCalendar date = new GregorianCalendar();
		date.setTimeInMillis(birthDay);
		int birthYear = date.get(GregorianCalendar.YEAR);
		
		int organizationDimension = locationOrganizationIdx | (birthYear << 1) | gender;		
		
		this.setDicElementId(organizationDimension,0);
		
		this.setDicElementId(user.getMainTagId(), 1);
		
		this.setDicElementId(user.getRandomIdx(),2);
		
		this.allocateFriendListMemory();
		
		// for user's agent information
		this.setHaveSmartPhone(user.isHaveSmartPhone);
		this.setAgentIdx(user.getAgentIdx());
		this.setBrowserIdx(user.getBrowserIdx());
		this.setIpAddress(user.getIpAddress());
		
		this.setNumPassFriends(user.getNumPassFriends());
		
		// DucPM: Need to check whether this info needs to be stored here
		this.setLocationIdx(user.getLocationIdx());
		this.setCityIdx(user.getCityIdx());
		this.setForumStatusId(user.getForumStatusId());
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
	
	public boolean isExistFriend(int friendId){
		return friendIds.contains(friendId);
	}
	
	public void setNumFriendsAdded(short numFriendsAdded) {
		this.numFriendsAdded = numFriendsAdded;
	}
	
	
	public void allocateFriendListMemory(){
		friendList = new Friend[numFriends];
		friendIds = new TreeSet<Integer>();
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

	public long getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(long createdDate) {
		this.createdDate = createdDate;
	}

	public int getAccountId() {
		return accountId;
	}
	public void setAccountId(int accountId) {
		this.accountId = accountId;
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
	public TreeSet<Integer> getFriendIds() {
		return friendIds;
	}
	public int[] getDicElementIds() {
		return dicElementIds;
	}
	public int getLocationOrganizationIdx() {
		return locationOrganizationIdx;
	}

	public void setLocationOrganizationIdx(int locationOrganizationIdx) {
		this.locationOrganizationIdx = locationOrganizationIdx;
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