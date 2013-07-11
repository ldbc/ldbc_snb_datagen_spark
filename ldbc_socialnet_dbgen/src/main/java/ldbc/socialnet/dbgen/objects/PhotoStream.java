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
import java.util.HashSet;

public class PhotoStream implements Serializable{
	boolean isPhoto = true; 
	long photoId;
	String image;
	
	long albumId; 
	int locationIdx; 
	int creatorId;		// Id of user's account
	String locationName; 
	double latt; 
	double longt; 
	//long takenTime;    // Replaced by createdDate
	HashSet<Integer> tags;
	int[] interestedUserAccs;
	
	IP ipAddress; 
	String userAgent;				// Send from where e.g., iPhone, Samsung, HTC
	
	byte 	browserIdx; 
	
	String title; 
	long createdDate;
	
	public PhotoStream(){}
	public PhotoStream(Photo photo){
		isPhoto = true; 
		this.photoId = photo.getPhotoId();
		this.image = photo.getImage(); 
		this.albumId = photo.getAlbumId(); 
		this.locationIdx = photo.getLocationIdx();
		this.creatorId = photo.getCreatorId();		// Id of user's account
		this.locationName = photo.getLocationName();
		this.latt = photo.getLatt(); 
		this.longt = photo.getLongt();
		this.createdDate = photo.getTakenTime();
		this.tags = photo.getTags();
		this.interestedUserAccs = photo.getInterestedUserAccs();
		this.ipAddress = photo.getIpAddress();
		this.userAgent = photo.getUserAgent();	
		this.browserIdx = photo.getBrowserIdx(); 
	}
	public Photo getPhoto(){
		Photo photo = new Photo();
		photo.setPhotoId(photoId);
		photo.setImage(image);
		photo.setAlbumId(albumId); 
		photo.setLocationIdx(locationIdx);
		photo.setCreatorId(creatorId);		// Id of user's account
		photo.setLocationName(locationName);
		photo.setLatt(latt); 
		photo.setLongt(longt);
		photo.setTakenTime(createdDate);
		photo.setTags(tags);
		photo.setInterestedUserAccs(interestedUserAccs);
		photo.setIpAddress(ipAddress);
		photo.setUserAgent(userAgent);	
		photo.setBrowserIdx(browserIdx); 
		
		return photo; 
	}

	public long getCreatedDate() {
		return createdDate;
	}
	public void setCreatedDate(long createdDate) {
		this.createdDate = createdDate;
	}
	public long getPhotoId() {
		return photoId;
	}
	public void setPhotoId(long photoId) {
		this.photoId = photoId;
	}
	public void setImage(String image) {
        this.image = image;
    }
    public String getImage() {
        return image;
    }
	public long getAlbumId() {
		return albumId;
	}
	public void setAlbumId(long albumId) {
		this.albumId = albumId;
	}
	public int getLocationIdx() {
		return locationIdx;
	}
	public void setLocationIdx(int locationIdx) {
		this.locationIdx = locationIdx;
	}
	public int getCreatorId() {
		return creatorId;
	}
	public void setCreatorId(int creatorId) {
		this.creatorId = creatorId;
	}
	public String getLocationName() {
		return locationName;
	}
	public void setLocationName(String locationName) {
		this.locationName = locationName;
	}
	public double getLatt() {
		return latt;
	}
	public void setLatt(double latt) {
		this.latt = latt;
	}
	public double getLongt() {
		return longt;
	}
	public void setLongt(double longt) {
		this.longt = longt;
	}
	public HashSet<Integer> getTags() {
		return tags;
	}
	public void setTags(HashSet<Integer>  tags) {
		this.tags = tags;
	}
	public int[] getInterestedUserAccs() {
		return interestedUserAccs;
	}
	public void setInterestedUserAccs(int[] interestedUserAccs) {
		this.interestedUserAccs = interestedUserAccs;
	}
	public IP getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(IP ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	public byte getBrowserIdx() {
		return browserIdx;
	}
	public void setBrowserIdx(byte browserIdx) {
		this.browserIdx = browserIdx;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public boolean isPhoto() {
		return isPhoto;
	}
	public void setPhoto(boolean isPhoto) {
		this.isPhoto = isPhoto;
	}

}
