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

import java.util.*;

/**
 * This class generates Forums (Walls, Groups and Albums).
 */
public class ForumGenerator {

    /**
     * Creates a personal wall for a given Person. All friends become members
     * @param randomFarm randomFarm
     * @param forumId forumID
     * @param person Person
     * @return Forum
     */
    Forum createWall(RandomGeneratorFarm randomFarm, long forumId, Person person) {

        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.getLanguages().size());

        Forum forum = new Forum(SN.formId(SN.composeId(forumId, person.getCreationDate() + DatagenParams.deltaTime)),
                                person.getCreationDate() + DatagenParams.deltaTime,
                                person.getDeletionDate(),
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Wall of " + person.getFirstName() + " " + person.getLastName(), 256),
                                person.getCityId(),
                                language
        );

        // wall inherits tags from person
        List<Integer> forumTags = new ArrayList<>(person.getInterests());
        forum.setTags(forumTags);

        // adds all friends as members of wall
        TreeSet<Knows> knows = person.getKnows();

        // for each friend generate hasMember edge
        for (Knows know : knows) {

            long hasMemberCreationDate = know.getCreationDate() + DatagenParams.deltaTime;
            long hasMemberDeletionDate = Math.min(forum.getDeletionDate(),know.getDeletionDate());

            forum.addMember(new ForumMembership(forum.getId(), hasMemberCreationDate, hasMemberDeletionDate ,know.to()));
        }
        return forum;
    }

    /**
     * Creates a Group with the Person as the moderator. 30% membership come from friends the rest are random.
     * @param randomFarm
     * @param forumId forumID
     * @param moderator moderator
     * @param block person block
     * @return Group
     */
    Forum createGroup(RandomGeneratorFarm randomFarm, long forumId, Person moderator, List<Person> block) {

        // creation date
        long groupCreationLowerBound = moderator.getCreationDate() + DatagenParams.deltaTime;
        long groupCreationUpperBound = Math.min(moderator.getDeletionDate(), Dictionaries.dates.getEndDateTime());
        long groupCreationDate = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), groupCreationLowerBound, groupCreationUpperBound);

        // deletion date
        long groupDeletionLowerBound = groupCreationDate + DatagenParams.deltaTime;
        long groupDeletionUpperBound = Dictionaries.dates.getStartDateTime() + DateUtils.TEN_YEARS;
        long groupDeletionDate = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), groupDeletionLowerBound, groupDeletionUpperBound);

        // push to network collapse for readability
        if (groupDeletionDate > Dictionaries.dates.getEndDateTime()) {
            groupDeletionDate = groupDeletionUpperBound;
        }

        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(moderator.getLanguages().size());

        Iterator<Integer> iter = moderator.getInterests().iterator();
        int idx = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_INTEREST).nextInt(moderator.getInterests().size());
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
                                new Person.PersonSummary(moderator),
                                StringUtils.clampString("Group for " + Dictionaries.tags.getName(interestId)
                                                                                        .replace("\"", "\\\"") + " in " + Dictionaries.places
                                        .getPlaceName(moderator.getCityId()), 256),
                                moderator.getCityId(),
                                language
        );

        // Set tags of this forum
        forum.setTags(interest);

        // Add members
        TreeSet<Long> groupMembers = new TreeSet<>();
        List<Knows> moderatorKnows = new ArrayList<>(moderator.getKnows());
        int numModeratorKnows = moderatorKnows.size();
        int groupSize = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_USERS_PER_FORUM).nextInt(DatagenParams.maxGroupSize);
        int numLoop = 0;
        while ((forum.getMemberships().size() < groupSize) && (numLoop < DatagenParams.blockSize)) {
            double prob = randomFarm.get(RandomGeneratorFarm.Aspect.KNOWS_LEVEL).nextDouble(); // controls the proportion of members that are friends
            if (prob < 0.3 && numModeratorKnows > 0) {
                // pick random knows edge from friends
                int knowsIndex = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(numModeratorKnows);
                Knows knows = moderatorKnows.get(knowsIndex);
                if (!groupMembers.contains(knows.to().getAccountId())) { // if friend not already member of group
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);

                    long creationDateLowerBound = Math.max(forum.getCreationDate(), knows.to().getCreationDate()) + DatagenParams.deltaTime;
                    long creationDateUpperBound = Collections.min(Arrays.asList(forum.getDeletionDate(), knows.to().getDeletionDate(),Dictionaries.dates.getEndDateTime()));
                    long hasMemberCreationDate = Dictionaries.dates.randomDate(random, creationDateLowerBound, creationDateUpperBound);

                    long deletionDateLowerBound = hasMemberCreationDate + DatagenParams.deltaTime;
                    long deletionDateUpperBound = Math.min(knows.to().getDeletionDate(),forum.getDeletionDate());
                    long hasMemberDeletionDate =  Dictionaries.dates.randomDate(random, deletionDateLowerBound, deletionDateUpperBound);

                    ForumMembership hasMember = new ForumMembership(forum.getId(), hasMemberCreationDate, hasMemberDeletionDate, knows.to());
                    forum.addMember(hasMember);
                    groupMembers.add(knows.to().getAccountId());
                }
            } else { // pick from the person block
                int candidateIndex = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX)
                                               .nextInt(block.size());
                Person member = block.get(candidateIndex);
                prob = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP).nextDouble();
                if ((prob < 0.1) && !groupMembers.contains(member.getAccountId())) {
                    groupMembers.add(member.getAccountId());
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);

                    long creationDateLowerBound = Math.max(forum.getCreationDate(), member.getCreationDate()) + DatagenParams.deltaTime;
                    long creationDateUpperBound = Math.min(forum.getDeletionDate(), member.getDeletionDate());
                    creationDateUpperBound = Math.min(creationDateUpperBound,Dictionaries.dates.getEndDateTime());
                    long hasMemberCreationDate = Dictionaries.dates.randomDate(random, creationDateLowerBound, creationDateUpperBound);

                    long deletionDateLowerBound = hasMemberCreationDate + DatagenParams.deltaTime;
                    long deletionDateUpperBound = Math.min(member.getDeletionDate(),forum.getDeletionDate());
                    long hasMemberDeletionDate =  Dictionaries.dates.randomDate(random, deletionDateLowerBound, deletionDateUpperBound);

                    forum.addMember(new ForumMembership(forum.getId(), hasMemberCreationDate, hasMemberDeletionDate, new Person.PersonSummary(member)));
                    groupMembers.add(member.getAccountId());
                }
            }
            numLoop++;
        }
        return forum;
    }

    /**
     * Creates an album for a given Person.
     * @param randomFarm random farm
     * @param forumId forumId
     * @param person Person who the album belongs it
     * @param numAlbum number of album e.g. Album 10
     * @return Album
     */
    Forum createAlbum(RandomGeneratorFarm randomFarm, long forumId, Person person, int numAlbum) {

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
        for (Knows knows : friends) {
            double prob = randomFarm.get(RandomGeneratorFarm.Aspect.ALBUM_MEMBERSHIP).nextDouble();
            if (prob < 0.7) { // add friends with prob

                long hasMemberCreationDate = Math.max(knows.to().getCreationDate(), forum.getCreationDate()) + DatagenParams.deltaTime;
                long hasMemberDeletionDate = Math.min(knows.to().getDeletionDate(), forum.getDeletionDate());

                forum.addMember(new ForumMembership(forum.getId(), hasMemberCreationDate, hasMemberDeletionDate, knows.to()));
            }
        }
        return forum;
    }
}
