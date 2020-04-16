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
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.util.StringUtils;
import ldbc.snb.datagen.vocabulary.SN;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/**
 * This class generates Forums (Walls, Groups and Albums).
 */
public class ForumGenerator {

    /**
     * Creates a personal wall for a given Person. All friends become members
     *
     * @param randomFarm randomFarm
     * @param forumId    forumID
     * @param person     Person
     * @return Forum
     */
    Forum createWall(RandomGeneratorFarm randomFarm, long forumId, Person person) {

        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.getLanguages().size());

        Forum forum = new Forum(SN.formId(SN.composeId(forumId, person.getCreationDate() + DatagenParams.deltaTime)),
                person.getCreationDate() + DatagenParams.deltaTime,
                person.getDeletionDate(),
                new PersonSummary(person),
                StringUtils.clampString("Wall of " + person.getFirstName() + " " + person.getLastName(), 256),
                person.getCityId(),
                language,
                Forum.ForumType.WALL,
                false);

        // wall inherits tags from person
        List<Integer> forumTags = new ArrayList<>(person.getInterests());
        forum.setTags(forumTags);

        // adds all friends as members of wall
        TreeSet<Knows> knows = person.getKnows();

        // for each friend generate hasMember edge
        for (Knows know : knows) {
            long hasMemberCreationDate = know.getCreationDate() + DatagenParams.deltaTime;
            long hasMemberDeletionDate = Math.min(forum.getDeletionDate(), know.getDeletionDate());
            forum.addMember(new ForumMembership(forum.getId(), hasMemberCreationDate, hasMemberDeletionDate, know.to(), Forum.ForumType.WALL, false));
        }
        return forum;
    }

    /**
     * Creates a Group with the Person as the moderator. 30% membership come from friends the rest are random.
     *
     * @param randomFarm random number generator
     * @param forumId    forumID
     * @param moderator  moderator
     * @param block      person block
     * @return Group
     */
    Forum createGroup(RandomGeneratorFarm randomFarm, long forumId, Person moderator, List<Person> block) {

        // creation date
        long groupMinCreationDate = moderator.getCreationDate() + DatagenParams.deltaTime;
        long groupMaxCreationDate = Math.min(moderator.getDeletionDate(), Dictionaries.dates.getSimulationEnd());
        long groupCreationDate = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), groupMinCreationDate, groupMaxCreationDate);

        //TODO: Currently if the group moderator is deleted before the forum then no new moderator is installed. This breaks the schema.

        // deletion date
        long groupDeletionDate;
        boolean isExplicitlyDeleted;
        if (randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_FORUM).nextDouble() < DatagenParams.probForumDeleted) {
            isExplicitlyDeleted = true;
            long groupMinDeletionDate = groupCreationDate + DatagenParams.deltaTime;
            long groupMaxDeletionDate = Dictionaries.dates.getSimulationEnd();
            groupDeletionDate = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), groupMinDeletionDate, groupMaxDeletionDate);
        } else {
            isExplicitlyDeleted = false;
            groupDeletionDate = Dictionaries.dates.getNetworkCollapse();
        }

        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(moderator.getLanguages().size());

        Iterator<Integer> iter = moderator.getInterests().iterator();
        int idx = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_INTEREST).nextInt(moderator.getInterests().size());
        for (int i = 0; i < idx; i++) {
            iter.next();
        }
        int interestId = iter.next();
        List<Integer> interest = new ArrayList<>();
        interest.add(interestId);

        // Create group
        Forum forum = new Forum(SN.formId(SN.composeId(forumId, groupCreationDate)),
                groupCreationDate,
                groupDeletionDate,
                new PersonSummary(moderator),
                StringUtils.clampString("Group for " + Dictionaries.tags.getName(interestId)
                        .replace("\"", "\\\"") + " in " + Dictionaries.places
                        .getPlaceName(moderator.getCityId()), 256),
                moderator.getCityId(),
                language,
                Forum.ForumType.GROUP,
                isExplicitlyDeleted
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

                    long minCreationDate = Math.max(forum.getCreationDate(), knows.to().getCreationDate()) + DatagenParams.deltaTime;
                    long maxCreationDate = Collections.min(Arrays.asList(forum.getDeletionDate(), knows.to().getDeletionDate(), Dictionaries.dates.getSimulationEnd()));

                    if (maxCreationDate - minCreationDate > 0) {

                        Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                        long hasMemberCreationDate = Dictionaries.dates.randomDate(random, minCreationDate, maxCreationDate);

                        long hasMemberDeletionDate;
                        boolean isHasMemberExplicitlyDeleted;
                        if (randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_MEMB).nextDouble() < DatagenParams.probMembDeleted) {
                            isHasMemberExplicitlyDeleted = true;
                            long minDeletionDate = hasMemberCreationDate + DatagenParams.deltaTime;
                            long maxDeletionDate = Collections.min(Arrays.asList(knows.to().getDeletionDate(), forum.getDeletionDate(), Dictionaries.dates.getSimulationEnd()));
                            hasMemberDeletionDate = Dictionaries.dates.randomDate(random, minDeletionDate, maxDeletionDate);
                        } else {
                            isHasMemberExplicitlyDeleted = false;
                            hasMemberDeletionDate = Collections.min(Arrays.asList(knows.to().getDeletionDate(), forum.getDeletionDate()));
                        }
                        ForumMembership hasMember = new ForumMembership(forum.getId(), hasMemberCreationDate, hasMemberDeletionDate, knows.to(), Forum.ForumType.GROUP, isHasMemberExplicitlyDeleted);
                        forum.addMember(hasMember);
                        groupMembers.add(knows.to().getAccountId());
                    }
                }
            } else { // pick from the person block
                int candidateIndex = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX)
                        .nextInt(block.size());
                Person member = block.get(candidateIndex);
                prob = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP).nextDouble();
                if ((prob < 0.1) && !groupMembers.contains(member.getAccountId())) {

                    long minHasMemberCreationDate = Math.max(forum.getCreationDate(), member.getCreationDate()) + DatagenParams.deltaTime;
                    long maxHasMemberCreationDate = Collections.min(Arrays.asList(forum.getDeletionDate(), member.getDeletionDate(), Dictionaries.dates.getSimulationEnd()));

                    if (maxHasMemberCreationDate - minHasMemberCreationDate > 0) {

                        Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                        long hasMemberCreationDate = Dictionaries.dates.randomDate(random, minHasMemberCreationDate, maxHasMemberCreationDate);

                        long hasMemberDeletionDate;
                        boolean isHasMemberExplicitlyDeleted;
                        if (randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_MEMB).nextDouble() < DatagenParams.probMembDeleted) {
                            isHasMemberExplicitlyDeleted = true;
                            long minHasMemberDeletionDate = hasMemberCreationDate + DatagenParams.deltaTime;
                            long maxHasMemberDeletionDate = Collections.min(Arrays.asList(member.getDeletionDate(), forum.getDeletionDate(), Dictionaries.dates.getSimulationEnd()));
                            hasMemberDeletionDate = Dictionaries.dates.randomDate(random, minHasMemberDeletionDate, maxHasMemberDeletionDate);
                        } else {
                            isHasMemberExplicitlyDeleted = false;
                            hasMemberDeletionDate = Collections.min(Arrays.asList(member.getDeletionDate(), forum.getDeletionDate()));
                        }
                        forum.addMember(new ForumMembership(forum.getId(), hasMemberCreationDate, hasMemberDeletionDate, new PersonSummary(member), Forum.ForumType.GROUP, isHasMemberExplicitlyDeleted));
                        groupMembers.add(member.getAccountId());
                    }
                }
            }
            numLoop++;
        }
        return forum;
    }

    /**
     * Creates an album for a given Person.
     *
     * @param randomFarm random farm
     * @param forumId    forumId
     * @param person     Person who the album belongs it
     * @param numAlbum   number of album e.g. Album 10
     * @return Album
     */
    Forum createAlbum(RandomGeneratorFarm randomFarm, long forumId, Person person, int numAlbum) {

        long minAlbumCreationDate = person.getCreationDate() + DatagenParams.deltaTime;
        long maxAlbumCreationDate = Math.min(person.getDeletionDate(), Dictionaries.dates.getSimulationEnd());
        long albumCreationDate = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), minAlbumCreationDate, maxAlbumCreationDate);

        long albumDeletionDate;
        boolean isExplicitlyDeleted;
        if (randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_FORUM).nextDouble() < DatagenParams.probForumDeleted) {
            isExplicitlyDeleted = true;
            long minAlbumDeletionDate = albumCreationDate + DatagenParams.deltaTime;
            long maxAlbumDeletionDate = Math.min(person.getDeletionDate(), Dictionaries.dates.getSimulationEnd());
            albumDeletionDate = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), minAlbumDeletionDate, maxAlbumDeletionDate);
        } else {
            isExplicitlyDeleted = false;
            albumDeletionDate = person.getDeletionDate();
        }


        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.getLanguages().size());
        Forum forum = new Forum(SN.formId(SN.composeId(forumId, albumCreationDate)),
                albumCreationDate,
                albumDeletionDate,
                new PersonSummary(person),
                StringUtils.clampString("Album " + numAlbum + " of " + person.getFirstName() + " " + person
                        .getLastName(), 256),
                person.getCityId(),
                language,
                Forum.ForumType.ALBUM,
                isExplicitlyDeleted
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
            if (prob < 0.7) {
                long hasMemberCreationDate = Math.max(knows.to().getCreationDate(), forum.getCreationDate()) + DatagenParams.deltaTime;
                long hasMemberDeletionDate = Collections.min(Arrays.asList(knows.to().getDeletionDate(), forum.getDeletionDate()));
                forum.addMember(new ForumMembership(forum.getId(), hasMemberCreationDate, hasMemberDeletionDate, knows.to(), Forum.ForumType.ALBUM, false
                ));
            }
        }
        return forum;
    }
}
