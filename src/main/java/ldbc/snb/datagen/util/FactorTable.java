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
import java.nio.charset.StandardCharsets;
import java.util.*;

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

        PersonCounts() {
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

        long numFriends() {
            return numFriends;
        }

        void numFriends(long numFriends) {
            this.numFriends = numFriends;
        }

        long numPosts() {
            return numPosts;
        }

        void incrNumPosts() {
            numPosts++;
        }

        long numLikes() {
            return numLikes;
        }

        void incrNumLikes() {
            numLikes++;
        }

        long numTagsOfMessages() {
            return numTagsOfMessages;
        }

        void numTagsOfMessages(long numTagsOfMessages) {
            this.numTagsOfMessages = numTagsOfMessages;
        }

        long numForums() {
            return numForums;
        }

        void incrNumForums() {
            numForums++;
        }

        long numWorkPlaces() {
            return numWorkPlaces;
        }

        void numWorkPlaces(long numWorkPlaces) {
            this.numWorkPlaces = numWorkPlaces;
        }

        long numComments() {
            return numComments;
        }

        void incrNumComments() {
            numComments++;
        }

        List<Long> numMessagesPerMonth() {
            return numMessagesPerMonth;
        }

        void incrNumMessagesPerMonth(int month) {
            numMessagesPerMonth.set(month, numMessagesPerMonth.get(month) + 1);
        }


        List<Long> numForumsPerMonth() {
            return numForumsPerMonth;
        }

        void incrNumForumsPerMonth(int month) {
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
            num = 0L;
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

    public void extractFactors(ForumMembership member) {
        long memberId = member.getPerson().getAccountId();
        personCounts(memberId).incrNumForums();
        int bucket = Dictionaries.dates
                .getNumberOfMonths(member.getCreationDate(), DatagenParams.startMonth, DatagenParams.startYear);
        if (bucket < 36 + 1)
            personCounts(memberId).incrNumForumsPerMonth(bucket);
    }

    public void extractFactors(Comment comment) {
        extractFactors((Message) comment);
        personCounts(comment.getAuthor().getAccountId()).incrNumComments();
    }

    public void extractFactors(Post post) {
        extractFactors((Message) post);
        personCounts(post.getAuthor().getAccountId()).incrNumPosts();
    }

    public void extractFactors(Photo photo) {
        extractFactors((Message) photo);
        personCounts(photo.getAuthor().getAccountId()).incrNumPosts();
    }

    private void extractFactors(Message message) {
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

    public void extractFactors(Like like) {
        personCounts(like.getPerson()).incrNumLikes();
    }

    public void writePersonFactors(OutputStream writer) {
        try {
            Map<Integer, List<String>> countryNames = new TreeMap<>();
            for (Map.Entry<Long, PersonCounts> c : personCounts.entrySet()) {
                if (c.getValue().name() != null) {
                    List<String> names = countryNames.computeIfAbsent(c.getValue().country(), k -> new ArrayList<>());
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
                String name = medianNames.get(c.getValue().country());
                if (name != null) {
                    StringBuilder strbuf = new StringBuilder();
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
                    writer.write(strbuf.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
            personCounts.clear();
            medianFirstName.clear();
        } catch (AssertionError | IOException e) {
            System.err.println("Unable to write parameter counts");
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void writeActivityFactors(OutputStream writer) throws IOException {
        try {
            writer.write(Integer.toString(postsPerCountry.size()).getBytes(StandardCharsets.UTF_8));
            writer.write("\n".getBytes(StandardCharsets.UTF_8));
            for (Map.Entry<Integer, Long> c : postsPerCountry.entrySet()) {
                String strbuf = Dictionaries.places.getPlaceName(c.getKey()) +
                        "," +
                        c.getValue() +
                        "\n";
                writer.write(strbuf.getBytes(StandardCharsets.UTF_8));
            }

            writer.write(Integer.toString(tagClassCount.size()).getBytes(StandardCharsets.UTF_8));
            writer.write("\n".getBytes(StandardCharsets.UTF_8));
            for (Map.Entry<Integer, Long> c : tagClassCount.entrySet()) {
                String strbuf = Dictionaries.tags.getClassName(c.getKey()) +
                        "," +
                        Dictionaries.tags.getClassName(c.getKey()) +
                        "," +
                        c.getValue() +
                        "\n";
                writer.write(strbuf.getBytes(StandardCharsets.UTF_8));
            }
            writer.write(Integer.toString(tagCount.size()).getBytes(StandardCharsets.UTF_8));
            writer.write("\n".getBytes(StandardCharsets.UTF_8));
            for (Map.Entry<Integer, Long> c : tagCount.entrySet()) {
                String strbuf = Dictionaries.tags.getName(c.getKey()) +
                        "," +
                        c.getValue() +
                        "\n";
                writer.write(strbuf.getBytes(StandardCharsets.UTF_8));
            }

            writer.write(Integer.toString(firstNameCount.size()).getBytes(StandardCharsets.UTF_8));
            writer.write("\n".getBytes(StandardCharsets.UTF_8));
            for (Map.Entry<String, Long> c : firstNameCount.entrySet()) {
                String strbuf = c.getKey() +
                        "," +
                        c.getValue() +
                        "\n";
                writer.write(strbuf.getBytes(StandardCharsets.UTF_8));
            }
            String strbuf = DatagenParams.startMonth +
                    "\n" +
                    DatagenParams.startYear +
                    "\n" +
                    Dictionaries.dates.formatYear(minWorkFrom) +
                    "\n" +
                    Dictionaries.dates.formatYear(maxWorkFrom) +
                    "\n";
            writer.write(strbuf.getBytes(StandardCharsets.UTF_8));
            writer.flush();
            writer.close();
        } catch (IOException e) {
            System.err.println("Unable to write parameter counts");
            System.err.println(e.getMessage());
            throw e;
        }
    }
}
