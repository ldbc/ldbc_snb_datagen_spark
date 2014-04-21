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
import ldbc.socialnet.dbgen.objects.*;
import ldbc.socialnet.dbgen.util.RandomGeneratorFarm;


public class PhotoGenerator {
	
	DateGenerator		dateGenerator;
	LocationDictionary locationDic;
	PopularPlacesDictionary dicPopularPlaces; 

	double				probPopularPlaces;
    long              deltaTime;
    private RandomGeneratorFarm randomFarm;
    private PowerDistGenerator likesGenerator;

    public PhotoGenerator(DateGenerator _dateGen, LocationDictionary locationDic,
						long _seed, PopularPlacesDictionary _dicPopularPlaces,
						double _probPopularPlaces, int maxNumberOfLikes, long deltaTime, RandomGeneratorFarm randomFarm){
		this.dateGenerator = _dateGen; 
		this.locationDic = locationDic; 
		this.dicPopularPlaces = _dicPopularPlaces;
		this.probPopularPlaces = _probPopularPlaces;
        this.deltaTime = deltaTime;
        this.randomFarm = randomFarm;
        this.likesGenerator = new PowerDistGenerator(1,maxNumberOfLikes,0.4);
	}
	
	public Photo generatePhoto(ReducedUserProfile user, Group album, 
								int idxInAlbum, long photoId){

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
			if (randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR).nextDouble() < probPopularPlaces){
				//Generate photo information from user's popular place
				int popularIndex = randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR).nextInt(numPopularPlace);
				popularPlaceId = user.getPopularId(popularIndex);
				popularPlace = dicPopularPlaces.getPopularPlace(user.getLocationId(),popularPlaceId);
				locationName = popularPlace.getName();
				latt = popularPlace.getLatt();
				longt = popularPlace.getLongt();
			} else{
				// Randomly select one places from Album location idx
				popularPlaceId = dicPopularPlaces.getPopularPlace(randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR),locationId);
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

        Photo photo = new Photo(photoId,"photo"+photoId+".jpg",0,album.getCreatedDate()+deltaTime+1000*(idxInAlbum+1),album.getModeratorId(),album.getGroupId(),tags,null,new String(""),(byte)-1,locationId,locationName,latt,longt);
        if( randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
            setLikes(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE),randomFarm.get(RandomGeneratorFarm.Aspect.DATE),photo,album);
        }
		return photo;
	}

    /** @brief Assigns a set of likes to a post created by a user.
     *  @param[in] group The group where the post was created.*/
    private void setLikes( Random randomNumLikes, Random randomDate, Message message, Group group) {
        int numMembers = group.getNumMemberAdded();
        int numLikes = likesGenerator.getValue(randomNumLikes);
        numLikes = numLikes >= numMembers ?  numMembers : numLikes;
        Like[] likes = new Like[numLikes];
        GroupMemberShip groupMembers[] = group.getMemberShips();
        int startIndex = 0;
        if( numLikes < numMembers ) {
            startIndex = randomNumLikes.nextInt(numMembers - numLikes);
        }
        for (int i = 0; i < numLikes; i++) {
            likes[i] = new Like();
            likes[i].user = groupMembers[startIndex+i].getUserId();
            likes[i].messageId = message.getMessageId();
            long minDate = message.getCreationDate() > groupMembers[startIndex+i].getJoinDate() ? message.getCreationDate() : groupMembers[startIndex+i].getJoinDate();
            likes[i].date = (long)(randomDate.nextDouble()*DateGenerator.SEVEN_DAYS+minDate+deltaTime);
            likes[i].type = 2;
        }
        message.setLikes(likes);
    }
}
