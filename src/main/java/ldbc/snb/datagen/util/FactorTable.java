package ldbc.snb.datagen.util;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by aprat on 1/8/15.
 */
public class FactorTable {

    static public class PersonCounts {
        private long numFriends_ = 0;
        private long numPosts_ = 0;
        private long numLikes_ = 0;
        private long numTagsOfMessages_ = 0;
        private long numForums_ = 0;
        private long numWorkPlaces_ = 0;
        private long numComments_ = 0;
        private ArrayList<Long> numMessagesPerMonth_ = null;
        private ArrayList<Long> numForumsPerMonth_ = null;

        public PersonCounts() {
            numMessagesPerMonth_ = new ArrayList<Long>(36+1);
            for( int i = 0; i < 36+1; ++i ) {
               numMessagesPerMonth_.add(new Long(0));
            }
            numForumsPerMonth_ = new ArrayList<Long>(36+1);
            for( int i = 0; i < 36+1; ++i ) {
                numForumsPerMonth_.add(new Long(0));
            }
        }


        public long numFriends() {
            return numFriends_;
        }

        public void numFriends( long numFriends ) {
            numFriends_ = numFriends;
        }

        public long numPosts() {
            return numPosts_;
        }

        public void numPosts(long numPosts) {
            numPosts_ = numPosts;
        }

        public void incrNumPosts() {
            numPosts_++;
        }

        public long numLikes() {
            return numLikes_;
        }

        public void numLikes( long numLikes ) {
            numLikes_ = numLikes;
        }

        public void incrNumLikes() {
            numLikes_++;
        }

        public long numTagsOfMessages () {
            return numTagsOfMessages_;
        }

        public void numTagsOfMessages( long numTagsOfMessages ) {
            numTagsOfMessages_ =  numTagsOfMessages;
        }

        public long numForums() {
            return numForums_;
        }

        public void incrNumForums() {
            numForums_++;
        }

        public void numForums( long numForums ) {
           numForums_ = numForums;
        }

        public long numWorkPlaces () {
            return numWorkPlaces_;
        }

        public void numWorkPlaces( long numWorkPlaces ) {
            numWorkPlaces_ = numWorkPlaces;
        }

        public long numComments() {
            return numComments_;
        }

        public void numComments( long numComments ) {
            numComments_ = numComments;
        }

        public void incrNumComments() {
            numComments_++;
        }

        public ArrayList<Long> numMessagesPerMonth( ) {
           return numMessagesPerMonth_;
        }

        public void numMessagesPerMonth( ArrayList<Long> numMessagesPerMonth) {
            numMessagesPerMonth_.clear();
            numMessagesPerMonth_.addAll(numMessagesPerMonth);
        }

        public void incrNumMessagesPerMonth( int month) {
            numMessagesPerMonth_.set(month,numMessagesPerMonth_.get(month)+1);
        }


        public ArrayList<Long> numForumsPerMonth() {
            return numForumsPerMonth_;
        }

        public void numGroupsPerMonth( ArrayList<Long> numForumsPerMonth) {
            numForumsPerMonth_.clear();
            numForumsPerMonth_ = numForumsPerMonth;
        }

        public void incrNumForumsPerMonth( int month) {
            numForumsPerMonth_.set(month,numForumsPerMonth_.get(month)+1);
        }
    }


    private HashMap<Long, PersonCounts > personCounts_;
    private HashMap<Integer, Long> postsPerCountry_;
    private HashMap<Integer, Long> tagClassCount_;
    private HashMap<String,Long> firstNameCount_;
    private HashMap<Integer,Long> tagCount_;
    private HashMap<Long, String> medianFirstName_;
    private long minWorkFrom_ = Long.MAX_VALUE;
    private long maxWorkFrom_ = Long.MIN_VALUE;

    public FactorTable() {
        personCounts_ = new HashMap<Long, PersonCounts >();
        postsPerCountry_ = new HashMap<Integer, Long>();
        tagClassCount_ = new HashMap<Integer, Long> ();
        firstNameCount_ = new HashMap<String, Long>();
        tagCount_ = new HashMap<Integer, Long>();
        medianFirstName_ = new HashMap<Long, String>();
    }

    private PersonCounts personCounts(Long id) {
        PersonCounts ret = personCounts_.get(id);
        if(ret == null) {
            ret = new FactorTable.PersonCounts();
            personCounts_.put(id, ret);
        }
        return ret;
    }

    private void incrPostPerCountry( int country ) {
        Long num = postsPerCountry_.get(country);
        if( num == null ) {
            num = new Long(0);
        }
        postsPerCountry_.put(country, ++num);
    }

    private void incrTagClassCount( int tagClass ) {
        Long num = tagClassCount_.get(tagClass);
        if( num == null ) {
            num = new Long(0);
        }
        tagClassCount_.put(tagClass,++num);
    }

    private void incrTagCount( int tag ) {
        Long num = tagCount_.get(tag);
        if( num == null ) {
            num = new Long(0);
        }
        tagCount_.put(tag, ++num);
    }

    private void incrFirstNameCount( String name ) {
        Long num = firstNameCount_.get(name);
        if( num == null ) {
            num = new Long(0);
        }
        firstNameCount_.put(name, ++num);
    }

    public void extractFactors( Person person ) {
        personCounts(person.accountId()).numFriends(person.knows().size());
        personCounts(person.accountId()).numWorkPlaces(person.companies().size());
        for( Map.Entry<Long,Long> e : person.companies().entrySet()) {
            if( minWorkFrom_ > e.getValue() )  minWorkFrom_ = e.getValue();
            if( maxWorkFrom_ < e.getValue() )  maxWorkFrom_ = e.getValue();
        }
        incrFirstNameCount(person.firstName());
        String medianName = Dictionaries.names.getMedianGivenName(person.countryId(), person.gender() == 1,
                Dictionaries.dates.getBirthYear(person.birthDay()));
        medianFirstName_.put(person.accountId(), medianName);
    }

    public void extractFactors( ForumMembership member ) {
        long memberId = member.person().accountId();
        personCounts(memberId).incrNumForums();
        int bucket = Dictionaries.dates.getNumberOfMonths(member.creationDate(),DatagenParams.startMonth, DatagenParams.startYear);
        if( bucket < 36 + 1)
            personCounts(memberId).incrNumForumsPerMonth(bucket);
    }

    public void extractFactors( Comment comment ) {
        extractFactors((Message)comment);
        personCounts(comment.author().accountId()).incrNumComments();
    }

    public void extractFactors( Post post ) {
        extractFactors((Message)post);
        personCounts(post.author().accountId()).incrNumPosts();
    }

    public void extractFactors( Photo photo ) {
        extractFactors((Message)photo);
        personCounts(photo.author().accountId()).incrNumPosts();
    }

    private void extractFactors( Message message ) {
        long authorId = message.author().accountId();
        long current = personCounts(authorId).numTagsOfMessages();
        personCounts(authorId).numTagsOfMessages(current + message.tags().size());
        int bucket = Dictionaries.dates.getNumberOfMonths(message.creationDate(),DatagenParams.startMonth, DatagenParams.startYear);
        if( bucket < 36 + 1)
            personCounts(authorId).incrNumMessagesPerMonth(bucket);
        incrPostPerCountry(message.countryId());
        for (Integer t: message.tags()){
            Integer tagClass = Dictionaries.tags.getTagClass(t);
            incrTagClassCount(tagClass);
            incrTagCount(t);
        }
    }

    public void extractFactors( Like like ) {
        personCounts(like.user).incrNumLikes();
    }

    public void writePersonFactors(OutputStream writer ) {
        try {
            for (Map.Entry<Long, PersonCounts> c: personCounts_.entrySet()){
                PersonCounts count = c.getValue();
                // correct the group counts
                //count.numberOfGroups += count.numberOfFriends;
                String name = medianFirstName_.get(c.getKey());
                if( name != null ) {
                    StringBuffer strbuf = new StringBuffer();
                    strbuf.append(c.getKey()); strbuf.append(",");
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
            personCounts_.clear();
            medianFirstName_.clear();
        } catch (IOException e) {
            System.err.println("Unable to write parameter counts");
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public void writeActivityFactors(OutputStream writer ) {
        try {
            writer.write(Integer.toString(postsPerCountry_.size()).getBytes("UTF8"));
            writer.write("\n".getBytes("UTF8"));
            for (Map.Entry<Integer, Long> c: postsPerCountry_.entrySet()){
            	StringBuffer strbuf = new StringBuffer();
            	strbuf.append(Dictionaries.places.getPlaceName(c.getKey()));
            	strbuf.append(",");
            	strbuf.append(c.getValue());
            	strbuf.append("\n");
            	writer.write(strbuf.toString().getBytes("UTF8"));
            }

            writer.write(Integer.toString(tagClassCount_.size()).getBytes("UTF8"));
            writer.write("\n".getBytes("UTF8"));
            for (Map.Entry<Integer, Long> c: tagClassCount_.entrySet()){
            	StringBuffer strbuf = new StringBuffer();
            	strbuf.append(Dictionaries.tags.getClassName(c.getKey()));
            	strbuf.append(",");
            	strbuf.append(Dictionaries.tags.getClassName(c.getKey()));
            	strbuf.append(",");
            	strbuf.append(c.getValue());
            	strbuf.append("\n");
            	writer.write(strbuf.toString().getBytes("UTF8"));
            }
            writer.write(Integer.toString(tagCount_.size()).getBytes("UTF8"));
            writer.write("\n".getBytes("UTF8"));
            for (Map.Entry<Integer, Long> c: tagCount_.entrySet()){
                StringBuffer strbuf = new StringBuffer();
                strbuf.append(Dictionaries.tags.getName(c.getKey()));
                strbuf.append(",");
                //strbuf.append(tagDictionary.getClassName(c.getKey()));
                //strbuf.append(",");
                strbuf.append(c.getValue());
                strbuf.append("\n");
                writer.write(strbuf.toString().getBytes("UTF8"));
            }

            writer.write(Integer.toString(firstNameCount_.size()).getBytes("UTF8"));
            writer.write("\n".getBytes("UTF8"));
            for (Map.Entry<String, Long> c: firstNameCount_.entrySet()){
            	StringBuffer strbuf = new StringBuffer();
            	strbuf.append(c.getKey());
            	strbuf.append(",");
            	strbuf.append(c.getValue());
            	strbuf.append("\n");
            	writer.write(strbuf.toString().getBytes("UTF8"));
            }
            StringBuffer strbuf = new StringBuffer();
            strbuf.append(DatagenParams.startMonth);	strbuf.append("\n");
            strbuf.append(DatagenParams.startYear);   strbuf.append("\n");
            strbuf.append(Dictionaries.dates.formatYear(minWorkFrom_)); strbuf.append("\n");
            strbuf.append(Dictionaries.dates.formatYear(maxWorkFrom_)); strbuf.append("\n");
            writer.write(strbuf.toString().getBytes("UTF8"));
            writer.flush();
            writer.close();
        } catch (IOException e) {
            System.err.println("Unable to write parameter counts");
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
