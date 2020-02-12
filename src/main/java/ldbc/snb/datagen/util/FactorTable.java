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
package ldbc.snb.datagen.util;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Message;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.TreeMap;

public class FactorTable {

    private Map<Long, PersonCounts> personCounts;
    private Map<Integer, Long> postsPerCountry;
    private Map<Integer, Long> tagClassCount;
    private Map<String, Long> firstNameCount;
    private Map<Integer, Long> tagCount;
    private Map<Long, String> medianFirstName;
    private long minWorkFrom = Long.MAX_VALUE;
    private long maxWorkFrom = Long.MIN_VALUE;

    static public class PersonCounts {
        private long numFriends = 0;
        private long numPosts = 0;
        private long numLikes = 0;
        private long numTagsOfMessages = 0;
        private long numForums = 0;
        private long numWorkPlaces = 0;
        private long numComments = 0;
        private int country = 0;
        private String name = null;
        private List<Long> numMessagesPerMonth;
        private List<Long> numForumsPerMonth;

        public PersonCounts() {
            numMessagesPerMonth = new ArrayList<>(36 + 1);
            for (int i = 0; i < 36 + 1; ++i) {
                numMessagesPerMonth.add(0L);
            }
            numForumsPerMonth = new ArrayList<>(36 + 1);
            for (int i = 0; i < 36 + 1; ++i) {
                numForumsPerMonth.add(0L);
            }
        }

        public int country() {
            return country;
        }

        public String name() {
            return name;
        }

        public void country(int country) {
            this.country = country;
        }

        public void name(String name) {
            this.name = name;
        }

        public long numFriends() {
            return numFriends;
        }

        public void numFriends(long numFriends) {
            this.numFriends = numFriends;
        }

        public long numPosts() {
            return numPosts;
        }

        public void numPosts(long numPosts) {
            this.numPosts = numPosts;
        }

        public void incrNumPosts() {
            numPosts++;
        }

        public long numLikes() {
            return numLikes;
        }

        public void numLikes(long numLikes) {
            this.numLikes = numLikes;
        }

        public void incrNumLikes() {
            numLikes++;
        }

        public long numTagsOfMessages() {
            return numTagsOfMessages;
        }

        public void numTagsOfMessages(long numTagsOfMessages) {
            this.numTagsOfMessages = numTagsOfMessages;
        }

        public long numForums() {
            return numForums;
        }

        public void incrNumForums() {
            numForums++;
        }

        public void numForums(long numForums) {
            this.numForums = numForums;
        }

        public long numWorkPlaces() {
            return numWorkPlaces;
        }

        public void numWorkPlaces(long numWorkPlaces) {
            this.numWorkPlaces = numWorkPlaces;
        }

        public long numComments() {
            return numComments;
        }

        public void numComments(long numComments) {
            this.numComments = numComments;
        }

        public void incrNumComments() {
            numComments++;
        }

        public List<Long> numMessagesPerMonth() {
            return numMessagesPerMonth;
        }

        public void numMessagesPerMonth(List<Long> numMessagesPerMonth) {
            this.numMessagesPerMonth.clear();
            this.numMessagesPerMonth.addAll(numMessagesPerMonth);
        }

        public void incrNumMessagesPerMonth(int month) {
            numMessagesPerMonth.set(month, numMessagesPerMonth.get(month) + 1);
        }


        public List<Long> numForumsPerMonth() {
            return numForumsPerMonth;
        }

        public void numGroupsPerMonth(List<Long> numForumsPerMonth) {
            this.numForumsPerMonth.clear();
            this.numForumsPerMonth = numForumsPerMonth;
        }

        public void incrNumForumsPerMonth(int month) {
            numForumsPerMonth.set(month, numForumsPerMonth.get(month) + 1);
        }
    }

    public FactorTable() {
        personCounts = new HashMap<>();
        postsPerCountry = new HashMap<>();
        tagClassCount = new HashMap<>();
        firstNameCount = new HashMap<>();
        tagCount = new HashMap<>();
        medianFirstName = new HashMap<>();
    }

    private PersonCounts personCounts(Long id) {
        PersonCounts ret = personCounts.get(id);
        if (ret == null) {
            ret = new FactorTable.PersonCounts();
            personCounts.put(id, ret);
        }
        return ret;
    }

    private void incrPostPerCountry(int country) {
        Long num = postsPerCountry.get(country);
        if (num == null) {
            num = 0L;
        }
        postsPerCountry.put(country, ++num);
    }

    private void incrTagClassCount(int tagClass) {
        Long num = tagClassCount.get(tagClass);
        if (num == null) {
            num = new Long(0);
        }
        tagClassCount.put(tagClass, ++num);
    }

    private void incrTagCount(int tag) {
        Long num = tagCount.get(tag);
        if (num == null) {
            num = 0L;
        }
        tagCount.put(tag, ++num);
    }

    private void incrFirstNameCount(String name) {
        Long num = firstNameCount.get(name);
        if (num == null) {
            num = 0L;
        }
        firstNameCount.put(name, ++num);
    }

    public void extractFactors(Person person) {
        if (person.getCreationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            personCounts(person.getAccountId()).country(person.getCountryId());
            personCounts(person.getAccountId()).name(person.getFirstName());
            personCounts(person.getAccountId()).numFriends(person.getKnows().size());
            personCounts(person.getAccountId()).numWorkPlaces(person.getCompanies().size());
            for (Map.Entry<Long, Long> e : person.getCompanies().entrySet()) {
                if (minWorkFrom > e.getValue()) minWorkFrom = e.getValue();
                if (maxWorkFrom < e.getValue()) maxWorkFrom = e.getValue();
            }
            incrFirstNameCount(person.getFirstName());
            String medianName = Dictionaries.names.getMedianGivenName(person.getCountryId(), person.getGender() == 1,
                                                                      Dictionaries.dates
                                                                              .getBirthYear(person.getBirthday()));
            medianFirstName.put(person.getAccountId(), medianName);
        }
    }

    public void extractFactors(ForumMembership member) {
        if (member.getCreationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            long memberId = member.getPerson().getAccountId();
            personCounts(memberId).incrNumForums();
            int bucket = Dictionaries.dates
                    .getNumberOfMonths(member.getCreationDate(), DatagenParams.startMonth, DatagenParams.startYear);
            if (bucket < 36 + 1)
                personCounts(memberId).incrNumForumsPerMonth(bucket);
        }
    }

    public void extractFactors(Comment comment) {
        if (comment.getCreationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            assert personCounts.get(comment.getAuthor()
                                            .getAccountId()) != null : "Person counts does not exist when extracting factors from comment";
            extractFactors((Message) comment);
            personCounts(comment.getAuthor().getAccountId()).incrNumComments();
        }
    }

    public void extractFactors(Post post) {
        if (post.getCreationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            assert (personCounts.get(post.getAuthor()
                                          .getAccountId()) != null) : "Person counts does not exist when extracting factors from post";
            extractFactors((Message) post);
            personCounts(post.getAuthor().getAccountId()).incrNumPosts();
        }
    }

    public void extractFactors(Photo photo) {
        if (photo.getCreationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            assert (personCounts.get(photo.getAuthor()
                                           .getAccountId()) != null) : "Person counts does not exist when extracting factors from photo";
            extractFactors((Message) photo);
            personCounts(photo.getAuthor().getAccountId()).incrNumPosts();
        }
    }

    private void extractFactors(Message message) {
        if (message.getCreationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            assert (personCounts.get(message.getAuthor()
                                             .getAccountId()) != null) : "Person counts does not exist when extracting factors from message";
            long authorId = message.getAuthor().getAccountId();
            long current = personCounts(authorId).numTagsOfMessages();
            personCounts(authorId).numTagsOfMessages(current + message.getTags().size());

            int bucket = Dictionaries.dates.getNumberOfMonths(
                                                    message.getCreationDate(),
                                                    DatagenParams.startMonth,
                                                    DatagenParams.startYear);

            if (bucket < 36 + 1)
                personCounts(authorId).incrNumMessagesPerMonth(bucket);


            incrPostPerCountry(message.getCountryId());
            for (Integer t : message.getTags()) {
                Integer tagClass = Dictionaries.tags.getTagClass(t);
                incrTagClassCount(tagClass);
                incrTagCount(t);
            }
        }
    }

    public void extractFactors(Like like) {
        if (like.date < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            assert (personCounts
                    .get(like.user) != null) : "Person counts does not exist when extracting factors from like";
            personCounts(like.user).incrNumLikes();
        }
    }

    public void writePersonFactors(OutputStream writer) {
        try {
            Map<Integer, List<String>> countryNames = new TreeMap<>();
            for (Map.Entry<Long, PersonCounts> c : personCounts.entrySet()) {
                if (c.getValue().name() != null) {
                    List<String> names = countryNames.get(c.getValue().country());
                    if (names == null) {
                        names = new ArrayList<>();
                        countryNames.put(c.getValue().country(), names);
                    }
                    names.add(c.getValue().name());
                }
            }
            Map<Integer, String> medianNames = new TreeMap<>();
            for (Map.Entry<Integer, List<String>> entry : countryNames.entrySet()) {
                Collections.sort(entry.getValue());
                medianNames.put(entry.getKey(), entry.getValue().get(entry.getValue().size() / 2));
            }
            for (Map.Entry<Long, PersonCounts> c : personCounts.entrySet()) {
                PersonCounts count = c.getValue();
                // correct the group counts
                //count.numberOfGroups += count.numberOfFriends;
                //String name = medianFirstName_.get(c.getKey());
                String name = medianNames.get(c.getValue().country());
                if (name != null) {
                    StringBuffer strbuf = new StringBuffer();
                    strbuf.append(c.getKey());
                    strbuf.append(",");
                    strbuf.append(name);
                    strbuf.append(",");
                    strbuf.append(count.numFriends());
                    strbuf.append(",");
                    strbuf.append(count.numPosts());
                    strbuf.append(",");
                    strbuf.append(count.numLikes());
                    strbuf.append(",");
                    strbuf.append(count.numTagsOfMessages());
                    strbuf.append(",");
                    strbuf.append(count.numForums());
                    strbuf.append(",");
                    strbuf.append(count.numWorkPlaces());
                    strbuf.append(",");
                    strbuf.append(count.numComments());
                    strbuf.append(",");

                    for (Long bucket : count.numMessagesPerMonth()) {
                        strbuf.append(bucket);
                        strbuf.append(",");
                    }
                    for (Long bucket : count.numForumsPerMonth()) {
                        strbuf.append(bucket);
                        strbuf.append(",");
                    }
                    strbuf.setCharAt(strbuf.length() - 1, '\n');
                    writer.write(strbuf.toString().getBytes("UTF8"));
                }
            }
            personCounts.clear();
            medianFirstName.clear();
        } catch (AssertionError e) {
            System.err.println("Unable to write parameter counts");
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("Unable to write parameter counts");
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void writeActivityFactors(OutputStream writer) throws IOException {
        try {
            writer.write(Integer.toString(postsPerCountry.size()).getBytes("UTF8"));
            writer.write("\n".getBytes("UTF8"));
            for (Map.Entry<Integer, Long> c : postsPerCountry.entrySet()) {
                StringBuffer strbuf = new StringBuffer();
                strbuf.append(Dictionaries.places.getPlaceName(c.getKey()));
                strbuf.append(",");
                strbuf.append(c.getValue());
                strbuf.append("\n");
                writer.write(strbuf.toString().getBytes("UTF8"));
            }

            writer.write(Integer.toString(tagClassCount.size()).getBytes("UTF8"));
            writer.write("\n".getBytes("UTF8"));
            for (Map.Entry<Integer, Long> c : tagClassCount.entrySet()) {
                StringBuffer strbuf = new StringBuffer();
                strbuf.append(Dictionaries.tags.getClassName(c.getKey()));
                strbuf.append(",");
                strbuf.append(Dictionaries.tags.getClassName(c.getKey()));
                strbuf.append(",");
                strbuf.append(c.getValue());
                strbuf.append("\n");
                writer.write(strbuf.toString().getBytes("UTF8"));
            }
            writer.write(Integer.toString(tagCount.size()).getBytes("UTF8"));
            writer.write("\n".getBytes("UTF8"));
            for (Map.Entry<Integer, Long> c : tagCount.entrySet()) {
                StringBuffer strbuf = new StringBuffer();
                strbuf.append(Dictionaries.tags.getName(c.getKey()));
                strbuf.append(",");
                //strbuf.append(tagDictionary.getClassName(c.getKey()));
                //strbuf.append(",");
                strbuf.append(c.getValue());
                strbuf.append("\n");
                writer.write(strbuf.toString().getBytes("UTF8"));
            }

            writer.write(Integer.toString(firstNameCount.size()).getBytes("UTF8"));
            writer.write("\n".getBytes("UTF8"));
            for (Map.Entry<String, Long> c : firstNameCount.entrySet()) {
                StringBuffer strbuf = new StringBuffer();
                strbuf.append(c.getKey());
                strbuf.append(",");
                strbuf.append(c.getValue());
                strbuf.append("\n");
                writer.write(strbuf.toString().getBytes("UTF8"));
            }
            StringBuffer strbuf = new StringBuffer();
            strbuf.append(DatagenParams.startMonth);
            strbuf.append("\n");
            strbuf.append(DatagenParams.startYear);
            strbuf.append("\n");
            strbuf.append(Dictionaries.dates.formatYear(minWorkFrom));
            strbuf.append("\n");
            strbuf.append(Dictionaries.dates.formatYear(maxWorkFrom));
            strbuf.append("\n");
            writer.write(strbuf.toString().getBytes("UTF8"));
            writer.flush();
            writer.close();
        } catch (IOException e) {
            System.err.println("Unable to write parameter counts");
            System.err.println(e.getMessage());
            throw e;
        }
    }
}
