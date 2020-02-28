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
package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.serializer.PersonActivityExporter;
import ldbc.snb.datagen.util.DateUtils;
import ldbc.snb.datagen.util.PersonBehavior;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/**
 * This class generates photos which are used in posts.
 */
class PhotoGenerator {

    private LikeGenerator likeGenerator;
    private Photo photo;

    PhotoGenerator(LikeGenerator likeGenerator) {
        this.likeGenerator = likeGenerator;
        this.photo = new Photo();
    }

    long createPhotos(RandomGeneratorFarm randomFarm, final Forum album, long numPhotosInAlbum, long startId, PersonActivityExporter exporter) throws IOException {
        long nextId = startId;
        int numPopularPlaces = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POPULAR)
                                         .nextInt(DatagenParams.maxNumPopularPlaces + 1);
        List<Short> popularPlaces = new ArrayList<>();
        for (int i = 0; i < numPopularPlaces; i++) {
            short aux = Dictionaries.popularPlaces.getPopularPlace(randomFarm
                                                                           .get(RandomGeneratorFarm.Aspect.POPULAR), album
                                                                           .getPlace());
            if (aux != -1) {
                popularPlaces.add(aux);
            }
        }
        for (int i = 0; i < numPhotosInAlbum; ++i) {

            TreeSet<Integer> tags = new TreeSet<>();

            // creates photo
            long creationDate = album.getCreationDate() + DatagenParams.deltaTime + 1000 * (i + 1);
            if (creationDate >= album.getDeletionDate()) {
                break;
            }

            Random randomDate = randomFarm.get(RandomGeneratorFarm.Aspect.DATE);
            long minDeletionDate = creationDate + DatagenParams.deltaTime;
            long maxDeletionDate = Math.min(album.getDeletionDate(), Dictionaries.dates.getNetworkCollapse());
            long deletionDate = Dictionaries.dates.randomDate(randomDate, minDeletionDate, maxDeletionDate);


            int country = album.getModerator().getCountryId();
            IP ip = album.getModerator().getIpAddress();
            Random random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);


            if (PersonBehavior.changeUsualCountry(random, creationDate)) {
                random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                country = Dictionaries.places.getRandomCountryUniform(random);
                random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                ip = Dictionaries.ips.getIP(random, country);
            }

            long id = SN.formId(SN.composeId(nextId++, creationDate));
            photo.initialize(id,
                              creationDate,
                              deletionDate,
                              album.getModerator(),
                              album.getId(),
                              "photo" + id + ".jpg",
                              tags,
                              country,
                              ip,
                              album.getModerator().getBrowserId()
            );
            exporter.export(photo);
            if (randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1) {
                likeGenerator.generateLikes(randomFarm
                                                     .get(RandomGeneratorFarm.Aspect.NUM_LIKE), album, photo, Like.LikeType.PHOTO, exporter);
            }
        }
        return nextId;
    }

}
