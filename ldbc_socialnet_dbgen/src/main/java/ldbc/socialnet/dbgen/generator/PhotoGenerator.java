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
package ldbc.socialnet.dbgen.generator;

import java.util.TreeSet;
import java.util.Random;

import ldbc.socialnet.dbgen.dictionary.LocationDictionary;
import ldbc.socialnet.dbgen.dictionary.PopularPlacesDictionary;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.objects.Photo;
import ldbc.socialnet.dbgen.objects.PopularPlace;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;


public class PhotoGenerator {
	
	DateGenerator		dateGenerator;
	LocationDictionary locationDic;
	PopularPlacesDictionary dicPopularPlaces; 
	Random 				rand;
	Random				randLikes;

	Random				randPopularPlaces;
	Random				randPopularPlacesId; 
	double				probPopularPlaces;			
	
	public PhotoGenerator(DateGenerator _dateGen, LocationDictionary locationDic, 
						long _seed, PopularPlacesDictionary _dicPopularPlaces,
						double _probPopularPlaces){
		this.dateGenerator = _dateGen; 
		this.locationDic = locationDic; 
		rand = new Random(_seed);
		randLikes = new Random(_seed);
		this.dicPopularPlaces = _dicPopularPlaces; 
		this.randPopularPlaces = new Random(_seed);
		this.randPopularPlacesId = new Random(_seed);
		this.probPopularPlaces = _probPopularPlaces;
	}
	
	public Photo generatePhoto(ReducedUserProfile user, Group album, 
								int idxInAlbum, int maxNumLikes, long photoId){

		int locationId = album.getLocationIdx();
        double latt = 0;
        double longt = 0;
        String locationName = "";
		byte numPopularPlace = user.getNumPopularPlace();
		if (numPopularPlace == 0){
			locationName = locationDic.getLocationName(locationId);
			latt = locationDic.getLatt(locationId);
			longt = locationDic.getLongt(locationId);
		} else{
			int popularPlaceId;
			PopularPlace popularPlace;
			if (randPopularPlaces.nextDouble() < probPopularPlaces){
				//Generate photo information from user's popular place
				int popularIndex = randPopularPlacesId.nextInt(numPopularPlace);
				popularPlaceId = user.getPopularId(popularIndex);
				popularPlace = dicPopularPlaces.getPopularPlace(user.getLocationId(),popularPlaceId);
				locationName = popularPlace.getName();
				latt = popularPlace.getLatt();
				longt = popularPlace.getLongt();
			} else{
				// Randomly select one places from Album location idx
				popularPlaceId = dicPopularPlaces.getPopularPlace(rand,locationId);
				if (popularPlaceId != -1){
					popularPlace = dicPopularPlaces.getPopularPlace(locationId, popularPlaceId);
					locationName = popularPlace.getName();
					latt = popularPlace.getLatt();
					longt = popularPlace.getLongt();
				} else{
					locationName = locationDic.getLocationName(locationId);
					latt = locationDic.getLatt(locationId);
					longt = locationDic.getLongt(locationId);
				}
			}
		}
		
		TreeSet<Integer> tags = new TreeSet<Integer>();
//        Iterator<Integer> it = user.getSetOfTags().iterator();
//        while (it.hasNext()) {
//            Integer value = it.next();
//            if (tags.isEmpty()) {
//                tags.add(value);
//            } else {
//                if (rand.nextDouble() < 0.2) {
//                    tags.add(value);
//                }
//            }
//        }

        Photo photo = new Photo(photoId,"photo"+photoId+".jpg",0,album.getCreatedDate()+1000*(idxInAlbum+1),album.getModeratorId(),album.getGroupId(),tags,null,new String(""),(byte)-1,locationId,locationName,latt,longt);

		int numberOfLikes = randLikes.nextInt(maxNumLikes);
		long[] likes = getFriendsLiked(album, numberOfLikes);
		photo.setInterestedUserAccs(likes);
        long[] likeTimestamp = new long[likes.length];
        for (int i = 0; i < likes.length; i++) {
            likeTimestamp[i] = (long)(rand.nextDouble()*DateGenerator.SEVEN_DAYS+photo.getCreationDate());
        }
        photo.setInterestedUserAccsTimestamp(likeTimestamp);
		
		return photo; 
	}
	
	public long[] getFriendsLiked(Group album, int numOfLikes){
		GroupMemberShip fullMembers[] = album.getMemberShips();
		
		long friends[];
		if (numOfLikes >= album.getNumMemberAdded()){
			friends = new long[album.getNumMemberAdded()];
			for (int j = 0; j < album.getNumMemberAdded(); j++){
				friends[j] = fullMembers[j].getUserId();
			}
		} else{
			friends = new long[numOfLikes];
			int startIdx = randLikes.nextInt(album.getNumMemberAdded() - numOfLikes);
			for (int j = 0; j < numOfLikes; j++){
				friends[j] = fullMembers[j+startIdx].getUserId();
			}			
		}
		return friends;
	}
}
