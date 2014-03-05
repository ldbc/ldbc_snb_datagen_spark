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

public class Group {
	int groupId; 
	long moderatorId; 		//creator Id
	long createdDate;
	
	int forumWallId; 
	int forumStatusId; 
	
	String groupName; 

	Integer[] tags;
	
	int locationIdx; 			// Each group is for one location which is the creator's location
	
	GroupMemberShip memberShips[]; 
	int numMemberAdded = 0; 

	public void initAllMemberships(int numMembers){
		memberShips = new GroupMemberShip[numMembers];
	}
	public void addMember(GroupMemberShip member){
		memberShips[numMemberAdded] = member;
		numMemberAdded++;
	}
	public int getGroupId() {
		return groupId;
	}
	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}
	public long getModeratorId() {
		return moderatorId;
	}
	public void setModeratorId(long moderatorId) {
		this.moderatorId = moderatorId;
	}
	public long getCreatedDate() {
		return createdDate;
	}
	public void setCreatedDate(long createdDate) {
		this.createdDate = createdDate;
	} 
	public Integer[] getTags() {
		return tags;
	}
	public void setTags(Integer[] tags) {
		this.tags = tags;
	}
	public String getGroupName() {
		return groupName;
	}
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}
	public GroupMemberShip[] getMemberShips() {
		return memberShips;
	}
	public void setMemberShips(GroupMemberShip[] memberShips) {
		this.memberShips = memberShips;
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
	public int getNumMemberAdded() {
		return numMemberAdded;
	}
	public void setNumMemberAdded(int numMemberAdded) {
		this.numMemberAdded = numMemberAdded;
	}
	public int getLocationIdx() {
		return locationIdx;
	}
	public void setLocationIdx(int locationIdx) {
		this.locationIdx = locationIdx;
	}
}
