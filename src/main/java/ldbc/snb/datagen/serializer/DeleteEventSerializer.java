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
import ldbc.snb.datagen.hadoop.DeleteEvent;
import ldbc.snb.datagen.hadoop.key.updatekey.DeleteEventKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;

import static ldbc.snb.datagen.entities.dynamic.relations.Like.LikeType.COMMENT;
import static ldbc.snb.datagen.hadoop.DeleteEvent.DeleteEventType.*;

public class DeleteEventSerializer {


    private SequenceFile.Writer[] streamWriter;
    private List<String> data;
    private DeleteEvent currentEvent;
    private int numPartitions;
    private int nextPartition = 0;
    private StringBuffer stringBuffer;
    private long currentDependantDate = 0;
    private Configuration conf;
    private DeleteStreamStats stats;
    private String fileNamePrefix;
    private int reducerId;

    private static class DeleteStreamStats {
        public long minDate = Long.MAX_VALUE;
        public long maxDate = Long.MIN_VALUE;
        public long count = 0;

        public long getMinDate() {
            return minDate;
        }

        public long getMaxDate() {
            return maxDate;
        }

        public long getCount() {
            return count;
        }

        public void setMinDate(long minDate) {
            this.minDate = minDate;
        }

        public void setMaxDate(long maxDate) {
            this.maxDate = maxDate;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    public DeleteEventSerializer(Configuration conf, String fileNamePrefix, int reducerId, int numPartitions) throws IOException {
        this.conf = conf;
        this.reducerId = reducerId;
        stringBuffer = new StringBuffer(512);
        data = new ArrayList<>();
        currentEvent = new DeleteEvent(-1, -1, DeleteEvent.DeleteEventType.NO_EVENT, "");
        this.numPartitions = numPartitions;
        stats = new DeleteStreamStats();
        this.fileNamePrefix = fileNamePrefix;
        streamWriter = new SequenceFile.Writer[this.numPartitions];
        FileContext fc = FileContext.getFileContext(conf);
        for (int i = 0; i < this.numPartitions; ++i) {
            Path outFile = new Path(this.fileNamePrefix + "_" + i);
            streamWriter[i] = SequenceFile
                    .createWriter(fc, conf, outFile, DeleteEventKey.class, Text.class, SequenceFile.CompressionType.NONE, new DefaultCodec(), new SequenceFile.Metadata(), EnumSet
                            .of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts
                            .checksumParam(Options.ChecksumOpt.createDisabled()));
            FileSystem fs = FileSystem.get(conf);
            Path propertiesFile = new Path(this.fileNamePrefix + ".properties");
            if (fs.exists(propertiesFile)) {
                FSDataInputStream file = fs.open(propertiesFile);
                Properties properties = new Properties();
                properties.load(file);
                stats.setMinDate(Long.parseLong(properties.getProperty("ldbc.snb.interactive.min_write_event_start_time")));
                stats.setMaxDate(Long.parseLong(properties.getProperty("ldbc.snb.interactive.max_write_event_start_time")));
                stats.setCount(Long.parseLong(properties.getProperty("ldbc.snb.interactive.num_events")));
                file.close();
                fs.delete(propertiesFile, true);
            }
        }
    }

    public void changePartition() {
        nextPartition = (++nextPartition) % numPartitions;
    }

    public void writeKeyValue(DeleteEvent event) throws IOException {
        if (event.getEventDate() <= Dictionaries.dates.getSimulationEnd()) {
            String string = event.getEventDate() +
                    "|" +
                    event.getDependantOnDate() +
                    "|" +
                    event.getDeleteEventType().getType() +
                    "|" +
                    event.getEventData() +
                    "\n";
            streamWriter[nextPartition]
                    .append(new DeleteEventKey(event.getEventDate(), reducerId, nextPartition), new Text(string));
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

    private void beginEvent(long date, DeleteEvent.DeleteEventType type) {
        stats.setMinDate(Math.min(stats.getMinDate(), date));
        stats.setMaxDate(Math.max(stats.getMaxDate(), date));
        stats.count++;
        currentEvent.setEventDate(date);
        currentEvent.setDependantOnDate(currentDependantDate);
        currentEvent.setDeleteEventType(type);
        currentEvent.setEventData(null);
        data.clear();
    }

    private void endEvent() throws IOException {
        currentEvent.setEventData(formatStringArray(data, "|"));
        writeKeyValue(currentEvent);
    }

    public void close() throws IOException {
        try {
            FileSystem fs = FileSystem.get(conf);
            for (int i = 0; i < numPartitions; ++i) {
                streamWriter[i].close();
            }

            if (DatagenParams.getDatagenMode() == DatagenMode.INTERACTIVE || DatagenParams.getDatagenMode() == DatagenMode.BI) {
                OutputStream output = fs.create(new Path(fileNamePrefix + ".properties"), true);
                output.write(("ldbc.snb.interactive.delete.gct_delta_duration:" + DatagenParams.deltaTime + "\n")
                        .getBytes());
                output.write(("ldbc.snb.interactive.delete.min_write_event_start_time:" + stats.getMinDate() + "\n")
                        .getBytes());
                output.write(("ldbc.snb.interactive.delete.max_write_event_start_time:" + stats.getMaxDate() + "\n")
                        .getBytes());
                if (stats.count != 0) {
                    output.write(("ldbc.snb.interactive.delete_interleave:" + (stats.getMaxDate() - stats.getMinDate()) / stats.getCount() + "\n")
                            .getBytes());
                } else {
                    output.write(("ldbc.snb.interactive.delete_interleave:" + "0" + "\n").getBytes());
                }
                output.write(("ldbc.snb.interactive.delete.num_events:" + stats.getCount()).getBytes());
                output.close();
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw e;
        }
    }

    public void export(Person person) throws IOException {

        currentDependantDate = person.getCreationDate();
        beginEvent(person.getDeletionDate(), REMOVE_PERSON);
        data.add(Long.toString(person.getAccountId()));
        endEvent();
    }

    public void export(Person p, Knows k) throws IOException {
        if (p.getAccountId() < k.to().getAccountId()) {
            currentDependantDate = k.getCreationDate();
            beginEvent(k.getDeletionDate(), REMOVE_FRIENDSHIP);
            data.add(Long.toString(p.getAccountId()));
            data.add(Long.toString(k.to().getAccountId()));
            endEvent();
        }
    }

    public void export(Post post) throws IOException {
        currentDependantDate = post.getCreationDate();
        beginEvent(post.getDeletionDate(), REMOVE_POST);
        data.add(Long.toString(post.getMessageId()));
        endEvent();
    }

    public void export(Like like) throws IOException {
        currentDependantDate = like.getCreationDate();
        if (like.getType() == COMMENT) {
            beginEvent(like.getDeletionDate(), REMOVE_LIKE_COMMENT);
        } else {
            beginEvent(like.getDeletionDate(), REMOVE_LIKE_POST);
        }
        data.add(Long.toString(like.getPerson()));
        data.add(Long.toString(like.getMessageId()));
        endEvent();
    }

    public void export(Photo photo) throws IOException {

        currentDependantDate = photo.getCreationDate();
        beginEvent(photo.getDeletionDate(), REMOVE_POST);
        data.add(Long.toString(photo.getMessageId()));
        endEvent();
    }

    public void export(Comment comment) throws IOException {

        currentDependantDate = comment.getCreationDate();
        beginEvent(comment.getDeletionDate(), REMOVE_COMMENT);
        data.add(Long.toString(comment.getMessageId()));
        endEvent();
    }

    public void export(Forum forum) throws IOException {
        currentDependantDate = forum.getCreationDate();
        beginEvent(forum.getDeletionDate(), REMOVE_FORUM);
        data.add(Long.toString(forum.getId()));
        endEvent();
    }

    public void export(ForumMembership membership) throws IOException {
        currentDependantDate = membership.getCreationDate();
        beginEvent(membership.getDeletionDate(), REMOVE_FORUM_MEMBERSHIP);
        data.add(Long.toString(membership.getForumId()));
        endEvent();
    }

}
