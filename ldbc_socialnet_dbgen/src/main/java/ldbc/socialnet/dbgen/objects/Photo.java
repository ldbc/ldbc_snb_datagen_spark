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

import java.util.TreeSet;

public class Photo {
    long photoId; 
    String image;
    long albumId; 
    int locationIdx; 
    long creatorId;		// Id of user's account
    String locationName;
    double latt; 
    double longt; 
    long takenTime; 
    TreeSet<Integer> tags;
    long[] interestedUserAccs;
    long[] interestedUserAccsTimestamp;

    IP ipAddress; 
    String userAgent;				// Send from where e.g., iPhone, Samsung, HTC

    byte 	browserIdx; 

    public long getCreatorId() {
        return creatorId;
    }
    public void setCreatorId(long creatorId) {
        this.creatorId = creatorId;
    }
    public byte getBrowserIdx() {
        return browserIdx;
    }
    public void setBrowserIdx(byte browserIdx) {
        this.browserIdx = browserIdx;
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
    public int getLocationId() {
        return locationIdx;
    }
    public void setLocationId(int locationIdx) {
        this.locationIdx = locationIdx;
    }
    public long getTakenTime() {
        return takenTime;
    }
    public void setTakenTime(long takenTime) {
        this.takenTime = takenTime;
    }
    public TreeSet<Integer>  getTags() {
        return tags;
    }
    public void setTags(TreeSet<Integer> tags) {
        this.tags = tags;
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
    public String getLocationName() {
        return locationName;
    }
    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }
    public long[] getInterestedUserAccs() {
        return interestedUserAccs;
    }
    public void setInterestedUserAccs(long[] interestedUserAccs) {
        this.interestedUserAccs = interestedUserAccs;
    }	
    public long[] getInterestedUserAccsTimestamp() {
        return interestedUserAccsTimestamp;
    }
    public void setInterestedUserAccsTimestamp(long[] timestamps) {
        this.interestedUserAccsTimestamp = timestamps;
    }
}
