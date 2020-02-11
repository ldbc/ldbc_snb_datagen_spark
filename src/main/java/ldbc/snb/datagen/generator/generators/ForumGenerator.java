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
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.util.DateUtils;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.util.StringUtils;
import ldbc.snb.datagen.vocabulary.SN;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/**
 * This class generates Forums.
 */
public class ForumGenerator {


    public Forum createWall(RandomGeneratorFarm randomFarm, long forumId, Person person) {
        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.getLanguages().size());

        Forum forum = new Forum(SN.formId(SN.composeId(forumId, person.getCreationDate() + DatagenParams.deltaTime)),
                                person.getCreationDate() + DatagenParams.deltaTime,
                                person.getDeletionDate(),
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Wall of " + person.getFirstName() + " " + person.getLastName(), 256),
                                person.getCityId(),
                                language
        );

        List<Integer> forumTags = new ArrayList<>(person.getInterests());
        forum.setTags(forumTags);

        TreeSet<Knows> knows = person.getKnows();
        for (Knows k : knows) {
            long date = Math.max(k.getCreationDate(), forum.getCreationDate()) + DatagenParams.deltaTime;
            assert (forum
                    .getCreationDate() + DatagenParams.deltaTime) <= date : "Forum creation date is larger than knows in wall " + forum
                    .getCreationDate() + " " + k.getCreationDate();
            forum.addMember(new ForumMembership(forum.getId(), date, k.to()));
        }
        return forum;
    }

    public Forum createGroup(RandomGeneratorFarm randomFarm, long forumId, Person person, List<Person> persons) {

        long groupCreationLowerBound = person.getCreationDate() + DatagenParams.deltaTime;
        long groupCreationUpperBound = Math.min(person.getDeletionDate(), Dictionaries.dates.getEndDateTime());
        long groupCreationDate = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), groupCreationLowerBound , groupCreationUpperBound);

        long groupDeletionLowerBound = groupCreationDate + DatagenParams.deltaTime;
        long groupDeletionUpperBound = Dictionaries.dates.getStartDateTime() + DateUtils.TEN_YEARS;
        long groupDeletionDate = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), groupDeletionLowerBound , groupDeletionUpperBound);

        // push to network collapse for readability
        if (groupDeletionDate > Dictionaries.dates.getEndDateTime()) {
            groupDeletionDate = groupDeletionUpperBound;
        }

        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.getLanguages().size());
        Iterator<Integer> iter = person.getInterests().iterator();
        int idx = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_INTEREST).nextInt(person.getInterests().size());
        for (int i = 0; i < idx; i++) {
            iter.next();
        }
        int interestId = iter.next().intValue();
        List<Integer> interest = new ArrayList<>();
        interest.add(interestId);

        // Create group
        Forum forum = new Forum(SN.formId(SN.composeId(forumId, groupCreationDate)),
                                groupCreationDate,
                                groupDeletionDate,
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Group for " + Dictionaries.tags.getName(interestId)
                                                                                        .replace("\"", "\\\"") + " in " + Dictionaries.places
                                        .getPlaceName(person.getCityId()), 256),
                                person.getCityId(),
                                language
        );

        // Set tags of this forum
        forum.setTags(interest);

        // Add members
        // TODO: lifespan managment
        TreeSet<Long> added = new TreeSet<>();
        List<Knows> friends = new ArrayList<>(person.getKnows());
        int numMembers = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_USERS_PER_FORUM).nextInt(DatagenParams.maxNumMemberGroup);
        int numLoop = 0;
        while ((forum.getMemberships().size() < numMembers) && (numLoop < DatagenParams.blockSize)) {
            double prob = randomFarm.get(RandomGeneratorFarm.Aspect.KNOWS_LEVEL).nextDouble();
            if (prob < 0.3 && person.getKnows().size() > 0) {
                int friendId = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(person.getKnows()
                                                                                                         .size());
                Knows k = friends.get(friendId);
                if (!added.contains(k.to().getAccountId())) {
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                    long date = Dictionaries.dates.randomDate(random, Math
                            .max(forum.getCreationDate(), k.getCreationDate() + DatagenParams.deltaTime));
                    assert forum
                            .getCreationDate() + DatagenParams.deltaTime <= date : "Forum creation date larger than membership date for knows based members";
                    forum.addMember(new ForumMembership(forum.getId(), date, k.to()));
                    added.add(k.to().getAccountId());
                }
            } else {
                int candidateIndex = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX)
                                               .nextInt(persons.size());
                Person member = persons.get(candidateIndex);
                prob = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP).nextDouble();
                if ((prob < 0.1) && !added.contains(member.getAccountId())) {
                    added.add(member.getAccountId());
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                    long date = Dictionaries.dates.randomDate(random,
                                                         Math.max(forum.getCreationDate(), member
                                                                 .getCreationDate()) + DatagenParams.deltaTime);
                    assert forum
                            .getCreationDate() + DatagenParams.deltaTime <= date : "Forum creation date larger than membership date for block based members";
                    forum.addMember(new ForumMembership(forum.getId(), date, new Person.PersonSummary(member)));
                    added.add(member.getAccountId());
                }
            }
            numLoop++;
        }
        return forum;
    }

    public Forum createAlbum(RandomGeneratorFarm randomFarm, long forumId, Person person, int numAlbum) {
//        long date = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), person
//                .getCreationDate() + DatagenParams.deltaTime);

        long albumCreationLowerBound = person.getCreationDate() + DatagenParams.deltaTime;
        long albumCreationUpperBound = Math.min(person.getDeletionDate(), Dictionaries.dates.getEndDateTime());
        long albumCreationDate = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), albumCreationLowerBound , albumCreationUpperBound);

        long albumDeletionDate = person.getDeletionDate();



        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.getLanguages().size());
        Forum forum = new Forum(SN.formId(SN.composeId(forumId, albumCreationDate)),
                                albumCreationDate,
                                albumDeletionDate,
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Album " + numAlbum + " of " + person.getFirstName() + " " + person
                                        .getLastName(), 256),
                                person.getCityId(),
                                language
        );

        Iterator<Integer> iter = person.getInterests().iterator();
        int idx = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_INTEREST).nextInt(person.getInterests().size());
        for (int i = 0; i < idx; i++) {
            iter.next();
        }
        int interestId = iter.next();
        List<Integer> interest = new ArrayList<>();
        interest.add(interestId);
        forum.setTags(interest);

        List<Integer> countries = Dictionaries.places.getCountries();
        int randomCountry = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY).nextInt(countries.size());
        forum.setPlace(countries.get(randomCountry));
        List<Knows> friends = new ArrayList<>(person.getKnows());
        for (Knows k : friends) {
            double prob = randomFarm.get(RandomGeneratorFarm.Aspect.ALBUM_MEMBERSHIP).nextDouble();
            if (prob < 0.7) {
                Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                long date = Dictionaries.dates.randomDate(random, Math.max(forum.getCreationDate(), k.getCreationDate())
                        + DatagenParams.deltaTime);
                forum.addMember(new ForumMembership(forum.getId(), date, k.to()));
            }
        }
        return forum;
    }
}
