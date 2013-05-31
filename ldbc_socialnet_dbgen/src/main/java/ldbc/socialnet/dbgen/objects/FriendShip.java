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


enum Status{
	initiator, 
	requested,
	approved
}
enum CorrelatedInfo{
	location, 
	interest
}
public class FriendShip extends SocialObject{
	Status status;
	CorrelatedInfo correlatedInfo;
	int userAcc01, userAcc02;
	long createdTime;
	
	public FriendShip(int userAcc1, int userAcc2){
		this.userAcc01 = userAcc1;
		this.userAcc02 = userAcc2;
	}
	public FriendShip(int userAcc1, int userAcc2, CorrelatedInfo correlatedInfo, long createdTime){
		this.userAcc01 = userAcc1;
		this.userAcc02 = userAcc2;
		this.correlatedInfo = correlatedInfo; 
		this.createdTime = createdTime;
	}
	public CorrelatedInfo getCorrelatedInfo() {
		return correlatedInfo;
	}
	public void setCorrelatedInfo(CorrelatedInfo correlatedInfo) {
		this.correlatedInfo = correlatedInfo;
	}	
	public Status getStatus() {
		return status;
	}
	public void setStatus(Status status) {
		this.status = status;
	}
	public int getUserAcc01() {
		return userAcc01;
	}
	public void setUserAcc01(int userAcc01) {
		this.userAcc01 = userAcc01;
	}
	public int getUserAcc02() {
		return userAcc02;
	}
	public void setUserAcc02(int userAcc02) {
		this.userAcc02 = userAcc02;
	}
	public long getCreatedTime() {
		return createdTime;
	}
	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}
}


