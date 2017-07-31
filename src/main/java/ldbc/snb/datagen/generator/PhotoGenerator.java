/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivityExporter;
import ldbc.snb.datagen.util.PersonBehavior;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;

/**
 * @author aprat
 */
public class PhotoGenerator {
    private LikeGenerator likeGenerator_;
    private Photo photo_;


    public PhotoGenerator(LikeGenerator likeGenerator) {
        this.likeGenerator_ = likeGenerator;
        this.photo_ = new Photo();
    }

    public long createPhotos(RandomGeneratorFarm randomFarm, final Forum album, final ArrayList<ForumMembership> memberships, long numPhotos, long startId, PersonActivityExporter exporter) throws IOException {
        long nextId = startId;
        int numPopularPlaces = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POPULAR)
                                         .nextInt(DatagenParams.maxNumPopularPlaces + 1);
        ArrayList<Short> popularPlaces = new ArrayList<Short>();
        for (int i = 0; i < numPopularPlaces; i++) {
            short aux = Dictionaries.popularPlaces.getPopularPlace(randomFarm
                                                                           .get(RandomGeneratorFarm.Aspect.POPULAR), album
                                                                           .place());
            if (aux != -1) {
                popularPlaces.add(aux);
            }
        }
        for (int i = 0; i < numPhotos; ++i) {
            int locationId = album.place();
            double latt = 0;
            double longt = 0;
            if (popularPlaces.size() == 0) {
                latt = Dictionaries.places.getLatt(locationId);
                longt = Dictionaries.places.getLongt(locationId);
            } else {
                int popularPlaceId;
                PopularPlace popularPlace;
                if (randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR).nextDouble() < DatagenParams.probPopularPlaces) {
                    //Generate photo information from user's popular place
                    int popularIndex = randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR).nextInt(popularPlaces.size());
                    popularPlaceId = popularPlaces.get(popularIndex);
                    popularPlace = Dictionaries.popularPlaces.getPopularPlace(album.place(), popularPlaceId);
                    latt = popularPlace.getLatt();
                    longt = popularPlace.getLongt();
                } else {
                    // Randomly select one places from Album location idx
                    popularPlaceId = Dictionaries.popularPlaces.getPopularPlace(randomFarm
                                                                                        .get(RandomGeneratorFarm.Aspect.POPULAR), locationId);
                    if (popularPlaceId != -1) {
                        popularPlace = Dictionaries.popularPlaces.getPopularPlace(locationId, popularPlaceId);
                        latt = popularPlace.getLatt();
                        longt = popularPlace.getLongt();
                    } else {
                        latt = Dictionaries.places.getLatt(locationId);
                        longt = Dictionaries.places.getLongt(locationId);
                    }
                }
            }
            TreeSet<Integer> tags = new TreeSet<Integer>();
            long date = album.creationDate() + DatagenParams.deltaTime + 1000 * (i + 1);
            int country = album.moderator().countryId();
            IP ip = album.moderator().ipAddress();
            Random random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);
            if (PersonBehavior.changeUsualCountry(random, date)) {
                random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                country = Dictionaries.places.getRandomCountryUniform(random);
                random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                ip = Dictionaries.ips.getIP(random, country);
            }

            long id = SN.formId(SN.composeId(nextId++, date));
            photo_.initialize(id,
                              date,
                              album.moderator(),
                              album.id(),
                              "photo" + id + ".jpg",
                              tags,
                              country,
                              ip,
                              album.moderator().browserId(),
                              latt,
                              longt);
            exporter.export(photo_);
            if (randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1) {
                likeGenerator_.generateLikes(randomFarm
                                                     .get(RandomGeneratorFarm.Aspect.NUM_LIKE), album, photo_, Like.LikeType.PHOTO, exporter);
            }
        }
        return nextId;
    }

}
