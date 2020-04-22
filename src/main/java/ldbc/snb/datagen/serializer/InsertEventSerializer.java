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
package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.DatagenMode;
import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.hadoop.InsertEvent;
import ldbc.snb.datagen.hadoop.key.updatekey.InsertEventKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;

public class InsertEventSerializer {

    private SequenceFile.Writer[] streamWriter;
    private List<String> data;
    private List<String> list;
    private InsertEvent currentEvent;
    private int numPartitions;
    private int nextPartition = 0;
    private StringBuffer stringBuffer;
    private long currentDependantDate = 0;
    private Configuration conf;
    private InsertStreamStats stats;
    private String fileNamePrefix;
    private int reducerId;

    private static class InsertStreamStats {
        public long minDate = Long.MAX_VALUE;
        public long maxDate = Long.MIN_VALUE;
        public long count = 0;
    }

    public InsertEventSerializer(Configuration conf, String fileNamePrefix, int reducerId, int numPartitions) throws IOException {
        this.conf = conf;
        this.reducerId = reducerId;
        stringBuffer = new StringBuffer(512);
        data = new ArrayList<>();
        list = new ArrayList<>();
        currentEvent = new InsertEvent(-1, -1, InsertEvent.InsertEventType.NO_EVENT, "");
        this.numPartitions = numPartitions;
        stats = new InsertStreamStats();
        this.fileNamePrefix = fileNamePrefix;
        streamWriter = new SequenceFile.Writer[this.numPartitions];
        FileContext fc = FileContext.getFileContext(conf);
        for (int i = 0; i < this.numPartitions; ++i) {
            Path outFile = new Path(this.fileNamePrefix + "_" + i);
            streamWriter[i] = SequenceFile
                    .createWriter(fc, conf, outFile, InsertEventKey.class, Text.class, CompressionType.NONE, new DefaultCodec(), new SequenceFile.Metadata(), EnumSet
                            .of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts
                            .checksumParam(Options.ChecksumOpt.createDisabled()));
            FileSystem fs = FileSystem.get(conf);
            Path propertiesFile = new Path(this.fileNamePrefix + ".properties");
            if (fs.exists(propertiesFile)) {
                FSDataInputStream file = fs.open(propertiesFile);
                Properties properties = new Properties();
                properties.load(file);
                stats.minDate = Long.parseLong(properties
                        .getProperty("ldbc.snb.interactive.min_write_event_start_time"));
                stats.maxDate = Long.parseLong(properties
                        .getProperty("ldbc.snb.interactive.max_write_event_start_time"));
                stats.count = Long.parseLong(properties.getProperty("ldbc.snb.interactive.num_events"));
                file.close();
                fs.delete(propertiesFile, true);
            }
        }
    }

    public void changePartition() {
        nextPartition = (++nextPartition) % numPartitions;
    }

    public void writeKeyValue(InsertEvent event) throws IOException {
        if (event.getEventDate() <= Dictionaries.dates.getSimulationEnd()) {
            String string = event.getEventDate() +
                    "|" +
                    event.getDependantOnDate() +
                    "|" +
                    (event.getInsertEventType().getType()) +
                    "|" +
                    event.getEventData() +
                    "\n";
            streamWriter[nextPartition]
                    .append(new InsertEventKey(event.getEventDate(), reducerId, nextPartition), new Text(string));
        }
    }

    private String formatStringArray(List<String> array, String separator) {
        if (array.size() == 0) return "";
        stringBuffer.setLength(0);
        for (String s : array) {
            stringBuffer.append(s);
            stringBuffer.append(separator);
        }
        return stringBuffer.substring(0, stringBuffer.length() - 1);
    }

    private void beginEvent(long date, InsertEvent.InsertEventType type) {
        stats.minDate = Math.min(stats.minDate, date);
        stats.maxDate = Math.max(stats.maxDate, date);
        stats.count++;
        currentEvent.setEventDate(date);
        currentEvent.setDependantOnDate(currentDependantDate);
        currentEvent.setInsertEventType(type);
        currentEvent.setEventData(null);
        data.clear();
    }

    private void endEvent() throws IOException {
        currentEvent.setEventData(formatStringArray(data, "|"));
        writeKeyValue(currentEvent);
    }

    private void beginList() {
        list.clear();
    }

    private void endList() {
        data.add(formatStringArray(list, ";"));
    }

    public void close() throws IOException {
        try {
            FileSystem fs = FileSystem.get(conf);
            for (int i = 0; i < numPartitions; ++i) {
                streamWriter[i].close();
            }

            if (DatagenParams.getDatagenMode() == DatagenMode.INTERACTIVE || DatagenParams.getDatagenMode() == DatagenMode.BI) {
                OutputStream output = fs.create(new Path(fileNamePrefix + ".properties"), true);
                output.write(("ldbc.snb.interactive.insert.gct_delta_duration:" + DatagenParams.delta + "\n")
                        .getBytes());
                output.write(("ldbc.snb.interactive.insert.min_write_event_start_time:" + stats.minDate + "\n")
                        .getBytes());
                output.write(("ldbc.snb.interactive.insert.max_write_event_start_time:" + stats.maxDate + "\n")
                        .getBytes());
                if (stats.count != 0) {
                    output.write(("ldbc.snb.interactive.insert_interleave:" + (stats.maxDate - stats.minDate) / stats.count + "\n")
                            .getBytes());
                } else {
                    output.write(("ldbc.snb.interactive.insert_interleave:" + "0" + "\n").getBytes());
                }
                output.write(("ldbc.snb.interactive.insert.num_events:" + stats.count).getBytes());
                output.close();
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw e;
        }
    }

    public void export(Person person) throws IOException {

        currentDependantDate = 0;
        beginEvent(person.getCreationDate(), InsertEvent.InsertEventType.ADD_PERSON);
        data.add(Long.toString(person.getAccountId()));
        data.add(person.getFirstName());
        data.add(person.getLastName());

        if (person.getGender() == 1) {
            data.add("male");
        } else {
            data.add("female");
        }
        data.add(Long.toString(person.getBirthday()));
        data.add(Long.toString(person.getCreationDate()));
        data.add(person.getIpAddress().toString());
        data.add(Dictionaries.browsers.getName(person.getBrowserId()));
        data.add(Integer.toString(person.getCityId()));

        beginList();
        for (Integer l : person.getLanguages()) {
            list.add(Dictionaries.languages.getLanguageName(l));
        }
        endList();

        beginList();
        list.addAll(person.getEmails());
        endList();

        beginList();
        for (Integer tag : person.getInterests()) {
            list.add(Integer.toString(tag));
        }
        endList();

        beginList();
        int universityId = person.getUniversityLocationId();
        if (universityId != -1 && person.getClassYear() != -1) {
            List<String> studyAtData = new ArrayList<>();
            studyAtData.add(Long.toString(Dictionaries.universities.getUniversityFromLocation(universityId)));
            studyAtData.add(Dictionaries.dates.formatYear(person.getClassYear()));
            list.add(formatStringArray(studyAtData, ","));
        }
        endList();

        beginList();
        for (Long companyId : person.getCompanies().keySet()) {
            List<String> workAtData = new ArrayList<>();
            workAtData.add(Long.toString(companyId));
            workAtData.add(Dictionaries.dates.formatYear(person.getCompanies().get(companyId)));
            list.add(formatStringArray(workAtData, ","));
        }
        endList();
        endEvent();
    }

    public void export(Person p, Knows k) throws IOException {
        if (p.getAccountId() < k.to().getAccountId()) {
            currentDependantDate = Math.max(p.getCreationDate(), k.to().getCreationDate());
            beginEvent(k.getCreationDate(), InsertEvent.InsertEventType.ADD_FRIENDSHIP);
            data.add(Long.toString(p.getAccountId()));
            data.add(Long.toString(k.to().getAccountId()));
            data.add(Long.toString(k.getCreationDate()));
            endEvent();
        }
    }

    public void export(Post post) throws IOException {
        currentDependantDate = post.getAuthor().getCreationDate();
        beginEvent(post.getCreationDate(), InsertEvent.InsertEventType.ADD_POST);
        String empty = "";
        data.add(Long.toString(post.getMessageId()));
        data.add(empty);
        data.add(Long.toString(post.getCreationDate()));
        data.add(post.getIpAddress().toString());
        data.add(Dictionaries.browsers.getName(post.getBrowserId()));
        data.add(Dictionaries.languages.getLanguageName(post.getLanguage()));
        data.add(post.getContent());
        data.add(Long.toString(post.getContent().length()));
        data.add(Long.toString(post.getAuthor().getAccountId()));
        data.add(Long.toString(post.getForumId()));
        data.add(Long.toString(post.getCountryId()));

        beginList();
        for (int tag : post.getTags()) {
            list.add(Integer.toString(tag));
        }
        endList();
        endEvent();
    }

    public void export(Like like) throws IOException {
        currentDependantDate = like.getPersonCreationDate();
        if (like.getType() == Like.LikeType.COMMENT) {
            beginEvent(like.getCreationDate(), InsertEvent.InsertEventType.ADD_LIKE_COMMENT);
        } else {
            beginEvent(like.getCreationDate(), InsertEvent.InsertEventType.ADD_LIKE_POST);
        }
        data.add(Long.toString(like.getPerson()));
        data.add(Long.toString(like.getMessageId()));
        data.add(Long.toString(like.getCreationDate()));
        endEvent();
    }

    public void export(Photo photo) throws IOException {

        currentDependantDate = photo.getAuthor().getCreationDate();
        beginEvent(photo.getCreationDate(), InsertEvent.InsertEventType.ADD_POST);
        String empty = "";
        data.add(Long.toString(photo.getMessageId()));
        data.add(photo.getContent());
        data.add(Long.toString(photo.getCreationDate()));
        data.add(photo.getIpAddress().toString());
        data.add(Dictionaries.browsers.getName(photo.getBrowserId()));
        data.add(empty);
        data.add(empty);
        data.add("0");
        data.add(Long.toString(photo.getAuthor().getAccountId()));
        data.add(Long.toString(photo.getForumId()));
        data.add(Long.toString(photo.getCountryId()));

        beginList();
        for (int tag : photo.getTags()) {
            list.add(Integer.toString(tag));
        }
        endList();
        endEvent();
    }

    public void export(Comment comment) throws IOException {

        currentDependantDate = comment.getAuthor().getCreationDate();
        beginEvent(comment.getCreationDate(), InsertEvent.InsertEventType.ADD_COMMENT);
        data.add(Long.toString(comment.getMessageId()));
        data.add(Long.toString(comment.getCreationDate()));
        data.add(comment.getIpAddress().toString());
        data.add(Dictionaries.browsers.getName(comment.getBrowserId()));
        data.add(comment.getContent());
        data.add(Integer.toString(comment.getContent().length()));
        data.add(Long.toString(comment.getAuthor().getAccountId()));
        data.add(Long.toString(comment.getCountryId()));
        if (comment.replyOf() == comment.postId()) {
            data.add(Long.toString(comment.postId()));
            data.add("-1");
        } else {
            data.add("-1");
            data.add(Long.toString(comment.replyOf()));
        }
        beginList();
        for (int tag : comment.getTags()) {
            list.add(Integer.toString(tag));
        }
        endList();
        endEvent();
    }

    public void export(Forum forum) throws IOException {
        currentDependantDate = forum.getModerator().getCreationDate();
        beginEvent(forum.getCreationDate(), InsertEvent.InsertEventType.ADD_FORUM);
        data.add(Long.toString(forum.getId()));
        data.add(forum.getTitle());
        data.add(Long.toString(forum.getCreationDate()));
        data.add(Long.toString(forum.getModerator().getAccountId()));

        beginList();
        for (int tag : forum.getTags()) {
            list.add(Integer.toString(tag));
        }
        endList();
        endEvent();
    }

    public void export(ForumMembership membership) throws IOException {
        currentDependantDate = membership.getPerson().getCreationDate();
        beginEvent(membership.getCreationDate(), InsertEvent.InsertEventType.ADD_FORUM_MEMBERSHIP);
        data.add(Long.toString(membership.getForumId()));
        data.add(Long.toString(membership.getPerson().getAccountId()));
        data.add(Long.toString(membership.getCreationDate()));
        endEvent();
    }

}
