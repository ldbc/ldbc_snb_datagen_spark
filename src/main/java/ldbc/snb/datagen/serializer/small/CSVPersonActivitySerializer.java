package ldbc.snb.datagen.serializer.small;

        import ldbc.snb.datagen.dictionary.Dictionaries;
        import ldbc.snb.datagen.objects.*;
        import ldbc.snb.datagen.serializer.HDFSCSVWriter;
        import ldbc.snb.datagen.serializer.PersonActivitySerializer;
        import org.apache.hadoop.conf.Configuration;

        import java.util.ArrayList;

/**
 *
 * @author aprat
 */
public class CSVPersonActivitySerializer extends PersonActivitySerializer {
    private HDFSCSVWriter [] writers;
    private ArrayList<String> arguments;
    private String empty="";

    private enum FileNames {
        USER_LIKES_MESSAGE ("user_likes_message"),
        MESSAGE("message"),
        USER_CREATES_MESSAGE("user_writes_message"),
        MESSAGE_HASTAG_TAG("message_tags_tag"),
        MESSAGE_REPLYOF_MESSAGE("message_replyOf_message");

        private final String name;

        private FileNames( String name ) {
            this.name = name;
        }
        public String toString() {
            return name;
        }
    }

    public CSVPersonActivitySerializer() {
    }

    @Override
    public void initialize(Configuration conf, int reducerId) {
        int numFiles = FileNames.values().length;
        writers = new HDFSCSVWriter[numFiles];
        for( int i = 0; i < numFiles; ++i) {
            writers[i] = new HDFSCSVWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"),FileNames.values()[i].toString()+"_"+reducerId,conf.getInt("ldbc.snb.datagen.numPartitions",1),conf.getBoolean("ldbc.snb.datagen.serializer.compressed",false),"|", conf.getBoolean("ldbc.snb.datagen.serializer.endlineSeparator",false));
        }
        arguments = new ArrayList<String>();


        arguments.add("User.id");
        arguments.add("Message.id");
        writers[FileNames.USER_LIKES_MESSAGE.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("id");
        arguments.add("body");
        arguments.add("date");
        writers[FileNames.MESSAGE.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("User.id");
        arguments.add("Message.id");
        writers[FileNames.USER_CREATES_MESSAGE.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Message.id");
        arguments.add("Tag.id");
        writers[FileNames.MESSAGE_HASTAG_TAG.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add("Message.id");
        arguments.add("Message.id");
        writers[FileNames.MESSAGE_REPLYOF_MESSAGE.ordinal()].writeEntry(arguments);
        arguments.clear();
    }

    @Override
    public void close() {
        int numFiles = FileNames.values().length;
        for(int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    protected void serialize( final Forum forum ) {

    }

    protected void serialize( final Post post ) {

        arguments.add(Long.toString(post.messageId()));
        arguments.add(post.content());
        arguments.add(Dictionaries.dates.formatDateTime(post.creationDate()));
        writers[FileNames.MESSAGE.ordinal()].writeEntry(arguments);
        arguments.clear();

        arguments.add(Long.toString(post.author().accountId()));
        arguments.add(Long.toString(post.messageId()));
        writers[FileNames.USER_CREATES_MESSAGE.ordinal()].writeEntry(arguments);
        arguments.clear();

        for( Integer t : post.tags() ) {
            arguments.add(Long.toString(post.messageId()));
            arguments.add(Integer.toString(t));
            writers[FileNames.MESSAGE_HASTAG_TAG.ordinal()].writeEntry(arguments);
            arguments.clear();
        }
    }

    protected void serialize( final Comment comment ) {
        arguments.add(Long.toString(comment.messageId()));
        arguments.add(comment.content());
        arguments.add(Dictionaries.dates.formatDateTime(comment.creationDate()));
        writers[FileNames.MESSAGE.ordinal()].writeEntry(arguments);
        arguments.clear();

        if (comment.replyOf() == comment.postId()) {
            arguments.add(Long.toString(comment.messageId()));
            arguments.add(Long.toString(comment.postId()));
            writers[FileNames.MESSAGE_REPLYOF_MESSAGE.ordinal()].writeEntry(arguments);
            arguments.clear();
        } else {
            arguments.add(Long.toString(comment.messageId()));
            arguments.add(Long.toString(comment.replyOf()));
            writers[FileNames.MESSAGE_REPLYOF_MESSAGE.ordinal()].writeEntry(arguments);
            arguments.clear();
        }

        arguments.add(Long.toString(comment.author().accountId()));
        arguments.add(Long.toString(comment.messageId()));
        writers[FileNames.USER_CREATES_MESSAGE.ordinal()].writeEntry(arguments);
        arguments.clear();

        for( Integer t : comment.tags() ) {
            arguments.add(Long.toString(comment.messageId()));
            arguments.add(Integer.toString(t));
            writers[FileNames.MESSAGE_HASTAG_TAG.ordinal()].writeEntry(arguments);
            arguments.clear();
        }
    }

    protected void serialize(final  Photo photo ) {

    }

    protected void serialize( final ForumMembership membership ) {
    }

    protected void serialize( final Like like ) {
        arguments.add(Long.toString(like.user));
        arguments.add(Long.toString(like.messageId));
        writers[FileNames.USER_LIKES_MESSAGE.ordinal()].writeEntry(arguments);
        arguments.clear();
    }
    public void reset() {

    }


}


