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
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.util.StringUtils;
import ldbc.snb.datagen.vocabulary.SN;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

public class ForumGenerator {

    public Forum createWall(RandomGeneratorFarm randomFarm, long forumId, Person person) {
        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.languages().size());
        Forum forum = new Forum(SN.formId(SN.composeId(forumId, person.creationDate() + DatagenParams.deltaTime)),
                                person.creationDate() + DatagenParams.deltaTime,
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Wall of " + person.firstName() + " " + person.lastName(), 256),
                                person.cityId(),
                                language
        );

        ArrayList<Integer> forumTags = new ArrayList<Integer>();
        for (Integer interest : person.interests()) {
            forumTags.add(interest);
        }
        forum.tags(forumTags);

        TreeSet<Knows> knows = person.knows();
        for (Knows k : knows) {
            long date = Math.max(k.creationDate(), forum.creationDate()) + DatagenParams.deltaTime;
            assert (forum
                    .creationDate() + DatagenParams.deltaTime) <= date : "Forum creation date is larger than knows in wall " + forum
                    .creationDate() + " " + k.creationDate();
            forum.addMember(new ForumMembership(forum.id(), date, k.to()));
        }
        return forum;
    }

    public Forum createGroup(RandomGeneratorFarm randomFarm, long forumId, Person person, ArrayList<Person> persons) {
        long date = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), person
                .creationDate() + DatagenParams.deltaTime);
        //if( date > Dictionaries.dates.getEndDateTime() )  return null;

        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.languages().size());
        Iterator<Integer> iter = person.interests().iterator();
        int idx = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_INTEREST).nextInt(person.interests().size());
        for (int i = 0; i < idx; i++) {
            iter.next();
        }
        int interestId = iter.next().intValue();
        ArrayList<Integer> interest = new ArrayList<Integer>();
        interest.add(interestId);

        Forum forum = new Forum(SN.formId(SN.composeId(forumId, date)),
                                date,
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Group for " + Dictionaries.tags.getName(interestId)
                                                                                        .replace("\"", "\\\"") + " in " + Dictionaries.places
                                        .getPlaceName(person.cityId()), 256),
                                person.cityId(),
                                language
        );

        //Set tags of this forum
        forum.tags(interest);


        TreeSet<Long> added = new TreeSet<Long>();
        ArrayList<Knows> friends = new ArrayList<Knows>();
        friends.addAll(person.knows());
        int numMembers = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_USERS_PER_FORUM)
                                   .nextInt(DatagenParams.maxNumMemberGroup);
        int numLoop = 0;
        while ((forum.memberships().size() < numMembers) && (numLoop < DatagenParams.blockSize)) {
            double prob = randomFarm.get(RandomGeneratorFarm.Aspect.KNOWS_LEVEL).nextDouble();
            if (prob < 0.3 && person.knows().size() > 0) {
                int friendId = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(person.knows()
                                                                                                         .size());
                Knows k = friends.get(friendId);
                if (!added.contains(k.to().accountId())) {
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                    date = Dictionaries.dates.randomDate(random, Math
                            .max(forum.creationDate(), k.creationDate() + DatagenParams.deltaTime));
                    assert forum
                            .creationDate() + DatagenParams.deltaTime <= date : "Forum creation date larger than membership date for knows based members";
                    /*if( date < Dictionaries.dates.getEndDateTime() )*/
                    {
                        forum.addMember(new ForumMembership(forum.id(), date, k.to()));
                        added.add(k.to().accountId());
                    }
                }
            } else {
                int candidateIndex = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX)
                                               .nextInt(persons.size());
                Person member = persons.get(candidateIndex);
                prob = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP).nextDouble();
                if ((prob < 0.1) && !added.contains(member.accountId())) {
                    added.add(member.accountId());
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                    date = Dictionaries.dates.randomDate(random,
                                                         Math.max(forum.creationDate(), member
                                                                 .creationDate()) + DatagenParams.deltaTime);
                    {
                        assert forum
                                .creationDate() + DatagenParams.deltaTime <= date : "Forum creation date larger than membership date for block based members";
                        forum.addMember(new ForumMembership(forum.id(), date, new Person.PersonSummary(member)));
                        added.add(member.accountId());
                    }
                }
            }
            numLoop++;
        }
        return forum;
    }

    public Forum createAlbum(RandomGeneratorFarm randomFarm, long forumId, Person person, int numAlbum) {
        long date = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), person
                .creationDate() + DatagenParams.deltaTime);
        //if( date > Dictionaries.dates.getEndDateTime() )  return null;
        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.languages().size());
        Forum forum = new Forum(SN.formId(SN.composeId(forumId, date)),
                                date,
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Album " + numAlbum + " of " + person.firstName() + " " + person
                                        .lastName(), 256),
                                person.cityId(),
                                language
        );

        Iterator<Integer> iter = person.interests().iterator();
        int idx = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_INTEREST).nextInt(person.interests().size());
        for (int i = 0; i < idx; i++) {
            iter.next();
        }
        int interestId = iter.next().intValue();
        ArrayList<Integer> interest = new ArrayList<Integer>();
        interest.add(interestId);
        forum.tags(interest);

        ArrayList<Integer> countries = Dictionaries.places.getCountries();
        int randomCountry = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY).nextInt(countries.size());
        forum.place(countries.get(randomCountry));
        ArrayList<Knows> friends = new ArrayList<Knows>();
        friends.addAll(person.knows());
        for (Knows k : friends) {
            double prob = randomFarm.get(RandomGeneratorFarm.Aspect.ALBUM_MEMBERSHIP).nextDouble();
            if (prob < 0.7) {
                Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                date = Dictionaries.dates.randomDate(random, Math.max(forum.creationDate(), k.creationDate())
                        + DatagenParams.deltaTime);
				/*if( date < Dictionaries.dates.getEndDateTime() )*/
                {
                    forum.addMember(new ForumMembership(forum.id(), date, k.to()));
                }
            }
        }
        return forum;
    }
}
