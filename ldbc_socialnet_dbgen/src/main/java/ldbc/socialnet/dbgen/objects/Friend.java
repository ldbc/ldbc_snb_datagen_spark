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

public class Friend implements Serializable{
	int friendAcc; 
	long createdTime;			//approved Time 
	long requestTime;			 
	long declinedTime; 
	byte passIdx;
	byte initiator; 			// 0: if user with smaller Id initiate the relationship, 1: if else
	
	
	//For user's agent information
	boolean				isHaveSmartPhone; 		// Use for providing the user agent information
	byte 				agentIdx; 				// Index of user agent in the dictionary, e.g., 0 for iPhone, 1 for HTC
	byte				browserIdx;				// Index of web browser, e.g., 0 for Internet Explorer
	
	boolean 			isFrequentChange;		
	IP	 				sourceIp; 				// Source IP address of the friend
	
	
	public Friend(int friendAcc, long _requestedTime, long _declinedTime, long _createdTime, byte passidx, byte initiator){
		this.friendAcc = friendAcc;
		this.requestTime = _requestedTime;
		this.declinedTime = _declinedTime;
		this.createdTime = _createdTime; 
		this.passIdx = passidx; 
		this.initiator = initiator;
	}
	public Friend(){}
	
	public void readFields(DataInput arg0) throws IOException{
		friendAcc = arg0.readInt();
		createdTime = arg0.readLong();
		requestTime = arg0.readLong();
		declinedTime = arg0.readLong(); 
		passIdx = arg0.readByte(); 
		initiator = arg0.readByte(); 
		
		isHaveSmartPhone = arg0.readBoolean();
		agentIdx = arg0.readByte();
		browserIdx = arg0.readByte(); 
		isFrequentChange = arg0.readBoolean();
		short ip1 = arg0.readShort();
		short ip2 = arg0.readShort();
		short ip3 = arg0.readShort();
		short ip4 = arg0.readShort();
		this.sourceIp = new IP(ip1, ip2, ip3, ip4); 
	}
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(friendAcc);
		arg0.writeLong(createdTime);
		arg0.writeLong(requestTime);
		arg0.writeLong(declinedTime);
		arg0.writeByte(passIdx);
		arg0.writeByte(initiator);
		
		arg0.writeBoolean(isHaveSmartPhone);
		arg0.writeByte(agentIdx);
		arg0.writeByte(browserIdx); 
		arg0.writeBoolean(isFrequentChange);
		arg0.writeShort(sourceIp.getIp1());
		arg0.writeShort(sourceIp.getIp2());
		arg0.writeShort(sourceIp.getIp3());
		arg0.writeShort(sourceIp.getIp4());
		
	}

	public Friend(ReducedUserProfile user, long _requestedTime, long _declinedTime, long _createdTime, 
	        byte passidx, byte initiator){
	    this.friendAcc = user.getAccountId();
	    this.requestTime = _requestedTime;
	    this.declinedTime = _declinedTime;
	    this.createdTime = _createdTime; 
	    this.passIdx = passidx; 
	    this.initiator = initiator;

	    this.isHaveSmartPhone = user.isHaveSmartPhone;
	    this.agentIdx = user.getAgentIdx();
	    this.browserIdx = user.getBrowserIdx();
	    this.isFrequentChange = user.isFrequentChange; 
	    this.setSourceIp(user.getIpAddress());
	}	
	
	
	public int getFriendAcc() {
		return friendAcc;
	}
	public void setFriendAcc(int friendAcc) {
		this.friendAcc = friendAcc;
	}
	public long getCreatedTime() {
		return createdTime;
	}
	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}
	public int getPassIdx() {
		return passIdx;
	}
	public void setPassIdx(byte passIdx) {
		this.passIdx = passIdx;
	} 
	public long getRequestTime() {
		return requestTime;
	}
	public void setRequestTime(long requestTime) {
		this.requestTime = requestTime;
	}
	public long getDeclinedTime() {
		return declinedTime;
	}
	public void setDeclinedTime(long declinedTime) {
		this.declinedTime = declinedTime;
	}
	public byte getInitiator() {
		return initiator;
	}
	public void setInitiator(byte initiator) {
		this.initiator = initiator;
	}
	public IP getSourceIp() {
		return sourceIp;
	}

	public void setSourceIp(IP sourceIp) {
		this.sourceIp = sourceIp;
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

}
