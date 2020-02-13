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
import ldbc.snb.datagen.generator.generators.postgenerators.FlashmobPostGenerator;
import ldbc.snb.datagen.generator.generators.postgenerators.UniformPostGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.LdbcSnbTextGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.PersonActivityExporter;
import ldbc.snb.datagen.serializer.UpdateEventSerializer;
import ldbc.snb.datagen.util.FactorTable;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PersonActivityGenerator {

    private long startForumId = 0;
    private long startMessageId = 0;

    private RandomGeneratorFarm randomFarm;
    private ForumGenerator forumGenerator;
    private UniformPostGenerator uniformPostGenerator;
    private FlashmobPostGenerator flashmobPostGenerator;
    private PhotoGenerator photoGenerator;
    private UpdateEventSerializer updateSerializer;
    private FactorTable factorTable;
    private PersonActivityExporter exporter;

    public PersonActivityGenerator(DynamicActivitySerializer dynamicActivitySerializer, UpdateEventSerializer updateSerializer) {

        randomFarm = new RandomGeneratorFarm();
        forumGenerator = new ForumGenerator();

        TextGenerator generator = new LdbcSnbTextGenerator(randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT), Dictionaries.tags);
        LikeGenerator likeGenerator = new LikeGenerator();
        CommentGenerator commentGenerator = new CommentGenerator(generator, likeGenerator);
        uniformPostGenerator = new UniformPostGenerator(generator, commentGenerator, likeGenerator);
        flashmobPostGenerator = new FlashmobPostGenerator(generator, commentGenerator, likeGenerator);
        photoGenerator = new PhotoGenerator(likeGenerator);
        this.updateSerializer = updateSerializer;

        factorTable = new FactorTable();
        exporter = new PersonActivityExporter(dynamicActivitySerializer, updateSerializer, factorTable);
    }

    private void generateActivity(Person person, List<Person> block) throws AssertionError, IOException {
        try {
            factorTable.extractFactors(person);
            generateWall(person);
            generateGroups(person, block);
            generateAlbums(person);
        } catch (AssertionError e) {
            System.out.println("Assertion error when generating activity!");
            System.out.println(e.getMessage());
            throw e;
        }
    }

    /**
     * Generates the personal wall for a Person. Note, only this Person creates Posts in the wall.
     * @param person Person
     * @throws IOException IOException
     */
    private void generateWall(Person person) throws IOException {

        // Generate wall
        Forum wall = forumGenerator.createWall(randomFarm, startForumId++, person);
        exporter.export(wall);

        for (ForumMembership fm : wall.getMemberships()) {
            exporter.export(fm);
        }

        // creates a forum membership for the moderator
        // only the moderator can post on their wall
        ForumMembership moderator = new ForumMembership(wall.getId(),
                                                wall.getCreationDate() + DatagenParams.deltaTime,
                                                            wall.getDeletionDate(),
                                                            new Person.PersonSummary(person));
        // list of members who can post on the wall - only moderator of wall can post on it
        List<ForumMembership> memberships = new ArrayList<>();
        memberships.add(moderator);

        // create posts
        startMessageId = uniformPostGenerator.createPosts(
                randomFarm, wall, memberships,
                numPostsPerGroup(randomFarm, wall, DatagenParams.maxNumPostPerMonth, DatagenParams.maxNumFriends),
                startMessageId, exporter);
        startMessageId = flashmobPostGenerator.createPosts(
                randomFarm, wall, memberships,
                numPostsPerGroup(randomFarm, wall, DatagenParams.maxNumFlashmobPostPerMonth, DatagenParams.maxNumFriends),
                startMessageId, exporter);
    }

    /**
     * Generates the Groups for a Person. Has 5% chance of becoming a moderator of some group(s).
     * @param person persons
     * @param block block for persons
     * @throws IOException IOException
     */
    private void generateGroups(Person person, List<Person> block) throws IOException {

        // generate person created groups
        double moderatorProb = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_MODERATOR).nextDouble();
        if (moderatorProb <= DatagenParams.groupModeratorProb) {
            int numGroup = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_FORUM)
                                      .nextInt(DatagenParams.maxNumGroupCreatedPerUser) + 1;
            for (int j = 0; j < numGroup; j++) {
                Forum group = forumGenerator.createGroup(randomFarm, startForumId++, person, block);
                exporter.export(group);

                for (ForumMembership fm : group.getMemberships()) {
                    exporter.export(fm);
                }

                // generate uniform posts/comments
                startMessageId = uniformPostGenerator.createPosts(
                        randomFarm,
                        group,
                        group.getMemberships(),
                        numPostsPerGroup(randomFarm, group, DatagenParams.maxNumGroupPostPerMonth, DatagenParams.maxGroupSize),
                        startMessageId,
                        exporter);
                startMessageId = flashmobPostGenerator.createPosts(
                        randomFarm,
                        group,
                        group.getMemberships(),
                        numPostsPerGroup(randomFarm, group, DatagenParams.maxNumGroupFlashmobPostPerMonth, DatagenParams.maxGroupSize),
                        startMessageId,
                        exporter);
            }
        }

    }

    /**
     * Generates the albums for a Person.
     * @param person person
     * @throws IOException IOException
     */
    private void generateAlbums(Person person) throws IOException {

        // work out number of albums to generate
        int numberOfMonths = (int) Dictionaries.dates.numberOfMonths(person);
        int numberOfPhotoAlbums = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_PHOTO_ALBUM).nextInt(DatagenParams.maxNumPhotoAlbumsPerMonth + 1);
        if (numberOfMonths != 0) {
            numberOfPhotoAlbums = numberOfMonths * numberOfPhotoAlbums;
        }

        //  create albums
        for (int i = 0; i < numberOfPhotoAlbums; i++) {
c
            Forum album = forumGenerator.createAlbum(randomFarm, startForumId++, person, i);
            exporter.export(album);

            for (ForumMembership fm : album.getMemberships()) {
                exporter.export(fm);
            }

            // number of photos to generate
            int numPhotos = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_PHOTO)
                                       .nextInt(DatagenParams.maxNumPhotoPerAlbums + 1);
            // create photos
            startMessageId = photoGenerator.createPhotos(randomFarm, album, numPhotos, startMessageId, exporter);
        }
    }

    private int numPostsPerGroup(RandomGeneratorFarm randomFarm, Forum forum, int maxPostsPerMonth, int maxMembersPerForum) {
        Random random = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST);
        int numOfMonths = (int) Dictionaries.dates.numberOfMonths(forum.getCreationDate());
        int numberPost = 0;
        if (numOfMonths == 0) {
            numberPost = random.nextInt(maxPostsPerMonth + 1);
        } else {
            numberPost = random.nextInt(maxPostsPerMonth * numOfMonths + 1);
        }
        return (numberPost * forum.getMemberships().size()) / maxMembersPerForum;
    }

    public void generateActivityForBlock(int seed, List<Person> block, Context context) throws IOException {
        randomFarm.resetRandomGenerators(seed);
        startForumId = 0;
        startMessageId = 0;
        SN.machineId = seed;
        int counter = 0;
        float personGenerationTime = 0.0f;
        for (Person p : block) {
            long start = System.currentTimeMillis();
            generateActivity(p, block);
            if (DatagenParams.updateStreams) {
                updateSerializer.changePartition();
            }
            if (counter % 1000 == 0) {
                context.setStatus("Generating activity of person " + counter + " of block" + seed);
                context.progress();
            }
            float time = (System.currentTimeMillis() - start) / 1000.0f;
            personGenerationTime += time;
            counter++;
        }
        System.out.println("Average person activity generation time " + personGenerationTime / (float) block.size());
    }

    public void writeActivityFactors(OutputStream writer) throws IOException {
        factorTable.writeActivityFactors(writer);
    }

    public void writePersonFactors(OutputStream writer) {
        factorTable.writePersonFactors(writer);
    }
}
