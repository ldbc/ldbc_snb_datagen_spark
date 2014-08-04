/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.socialnet.dbgen.serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.LanguageDictionary;
import ldbc.socialnet.dbgen.dictionary.TagDictionary;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.*;
import ldbc.socialnet.dbgen.vocabulary.SN;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

/**
 * Created by aprat on 3/27/14.
 */
public class UpdateEventSerializer implements Serializer{

    private OutputStream fileOutputStream;
    private SequenceFile.Writer hdfsWriter;
    private ArrayList<Object> data;
    private ArrayList<Object> list;
    private UpdateEvent currentEvent;
    private GregorianCalendar date;
    private BrowserDictionary browserDic;
    private LanguageDictionary languageDic;
    private IPAddressDictionary ipDic;
    private TagDictionary tagDic;
    private boolean exportText;
    private Statistics statistics;
    private long minDate;
    private long maxDate;
    private Gson gson;
    private long numEvents = 0;

    public UpdateEventSerializer( String outputDir, String outputFileName,boolean exportText, boolean compress, TagDictionary tagDic, BrowserDictionary browserDic, LanguageDictionary languageDic, IPAddressDictionary ipDic, Statistics statistics) {
        gson = new GsonBuilder().disableHtmlEscaping().create();
        this.data = new ArrayList<Object>();
        this.currentEvent = new UpdateEvent(-1, UpdateEvent.UpdateEventType.NO_EVENT,new String(""));
        this.date = new GregorianCalendar();
        this.browserDic = browserDic;
        this.languageDic = languageDic;
        this.ipDic = ipDic;
        this.exportText = exportText;
        this.tagDic = tagDic;
        this.statistics = statistics;
        this.minDate = Long.MAX_VALUE;
        this.maxDate = Long.MIN_VALUE;
        try{
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            /*if( compress ) {
                this.fileOutputStream = new GZIPOutputStream(new FileOutputStream(outputDir + "/" + outputFileName +".gz"));
            } else {
                this.fileOutputStream = new FileOutputStream(outputDir + "/" + outputFileName );
            }
            hdfsOutput = new FSDataOutputStream(this.fileOutputStream, new FileSystem.Statistics(null));
            */
            Path outFile = new Path(outputDir + "/" + outputFileName);
            hdfsWriter = new SequenceFile.Writer(fs, conf,outFile, LongWritable.class,Text.class);
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(-1);
        }
        this.statistics = statistics;
        ArrayList<String> params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_PERSON.toString());
        params.add("UserId");
        params.add("FirstName");
        params.add("LastName");
        params.add("Gender");
        params.add("BirthDay");
        params.add("CreationDate");
        params.add("Ip");
        params.add("Browser");
        params.add("Location");
        params.add("Languages");
        params.add("Emails");
        params.add("Tags");
        params.add("SudyAt");
        params.add("WorkAt");
//        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_LIKE_POST.toString());
        params.add("UserId");
        params.add("PostId");
        params.add("CreationDate");
//        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_LIKE_COMMENT.toString());
        params.add("UserId");
        params.add("CommentId");
        params.add("CreationDate");
//        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_FORUM.toString());
        params.add("ForumId");
        params.add("ForumTitle");
        params.add("CreationDate");
        params.add("ModeratorId");
        params.add("Tags");
//        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_FORUM_MEMBERSHIP.toString());
        params.add("ForumId");
        params.add("UserId");
        params.add("CreationDate");
//        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_POST.toString());
        params.add("PostId");
        params.add("ImageFile");
        params.add("CreationDate");
        params.add("Ip");
        params.add("Browser");
        params.add("Language");
        params.add("Content");
        params.add("Length");
        params.add("AuthorId");
        params.add("ForumId");
        params.add("Location");
        params.add("Tags");
//        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_COMMENT.toString());
        params.add("CommentId");
        params.add("CreationDate");
        params.add("Ip");
        params.add("Browser");
        params.add("Content");
        params.add("Length");
        params.add("AuthorId");
        params.add("Location");
        params.add("ReplyOfPost");
        params.add("ReplyOfComment");
        params.add("Tags");
//        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_FRIENDSHIP.toString());
        params.add("UserId");
        params.add("UserId");
        params.add("CreationDate");
//        statistics.eventParams.add(params);
    }


    public void writeKeyValue( UpdateEvent event ) {
        try{
            StringBuffer string = new StringBuffer();
            string.append(Long.toString(event.date));
            string.append("|");
            string.append(event.type.toString());
            string.append("|");
            string.append(event.eventData);
            string.append("|");
            string.append("\n");
            //fileOutputStream.write(string.toString().getBytes("UTF8"));
            hdfsWriter.append(new LongWritable(event.date),new Text(string.toString()));
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    private void beginEvent( long date, UpdateEvent.UpdateEventType type ) {
        if( date < minDate ) minDate = date;
        if( date > maxDate ) maxDate = date;
        currentEvent.date = date;
        currentEvent.type = type;
        currentEvent.eventData = null;
        data.clear();
    }

    private void endEvent() {
        numEvents++;
        currentEvent.eventData = gson.toJson(data);
        writeKeyValue(currentEvent);
    }

    private void beginList() {
        list = new ArrayList<Object>();
        list.clear();
    }

    private void endList() {
        data.add(list);
    }


    public void close() {
        statistics.minUpdateStream.add(minDate);
        date.setTimeInMillis(minDate);
        statistics.minUpdateStream.add(DateGenerator.formatDateDetail(date));

        statistics.maxUpdateStream.add(maxDate);
        date.setTimeInMillis(maxDate);
        statistics.maxUpdateStream.add(DateGenerator.formatDateDetail(date));
        System.out.println("Number of update events serialized "+numEvents);

        try {
            hdfsWriter.close();
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }


/*    private String formatData( ArrayList<String> data) {
        StringBuffer string = new StringBuffer(1024);
        string.append("{");
        for( int i = 0; i < data.size() - 1; ++i ) {
            string.append("\"");
            string.append(data.get(i));
            string.append("\"");
            string.append(",");
        }
        string.append("\"");
        string.append(data.get(data.size()-1));
        string.append("\"");
        string.append("}");
        return string.toString();
    }
    */

   /* private String createList( ArrayList<String> data) {
        StringBuffer string = new StringBuffer(1024);
        string.append("[");
        for( int i = 0; i < data.size() - 1; ++i ) {
            string.append(data.get(i));
            string.append(",");
        }
        if( data.size() > 0 ) {
            string.append(data.get(data.size()-1));
        }
        string.append("]");
        return string.toString();
    }
    */

    @Override
    public Long unitsGenerated() {
        return new Long(0);
    }

    @Override
    public void serialize(UserInfo info) {

        beginEvent(info.user.getCreationDate(), UpdateEvent.UpdateEventType.ADD_PERSON);
        data.add(info.user.getAccountId());
        data.add(info.extraInfo.getFirstName());
        data.add(info.extraInfo.getLastName());
        data.add(info.extraInfo.getGender());
        date.setTimeInMillis(info.user.getBirthDay());
        String dateString = DateGenerator.formatDate(date);
        data.add(dateString);
        date.setTimeInMillis(info.user.getCreationDate());
        dateString = DateGenerator.formatDateDetail(date);
        data.add(dateString);
        if (info.user.getIpAddress() != null) {
            data.add(info.user.getIpAddress().toString());
        } else {
            String empty = "";
            data.add(empty);
        }
        if (info.user.getBrowserId() >= 0) {
            data.add(browserDic.getName(info.user.getBrowserId()));
        } else {
            String empty = "";
            data.add(empty);
        }
        data.add(info.extraInfo.getLocationId());
        ArrayList<Object> languages = new ArrayList<Object>();
        Vector<Integer> userLang = info.extraInfo.getLanguages();
        for (int i = 0; i < languages.size(); i++) {
            languages.add(languageDic.getLanguagesName(userLang.get(i)));
        }
        data.add(languages);

        beginList();
        Iterator<String> itString = info.extraInfo.getEmail().iterator();
        while (itString.hasNext()){
            list.add(itString.next());
        }
        endList();

        beginList();
        Iterator<Integer> itInteger = info.user.getInterests().iterator();
        while (itInteger.hasNext()){
            Integer interestIdx = itInteger.next();
            list.add(interestIdx);
        }
        endList();


        beginList();
        long universityId = info.extraInfo.getUniversity();
        if ( universityId != -1){
            if (info.extraInfo.getClassYear() != -1 ) {
                ArrayList<Object> studyAtData = new ArrayList<Object>();
                date.setTimeInMillis(info.extraInfo.getClassYear());
                dateString = DateGenerator.formatYear(date);
                studyAtData.add(universityId);
                studyAtData.add(Integer.parseInt(dateString));
                list.add(studyAtData);
            }
        }
        endList();

        beginList();
        Iterator<Long> it = info.extraInfo.getCompanies().iterator();
        while (it.hasNext()) {
            long companyId = it.next();
            date.setTimeInMillis(info.extraInfo.getWorkFrom(companyId));
            ArrayList<Object> workAtData = new ArrayList<Object>();
            dateString = DateGenerator.formatYear(date);
            workAtData.add(companyId);
            workAtData.add(Integer.parseInt(dateString));
            list.add(workAtData);
        }
        endList();
        endEvent();
    }

    @Override
    public void serialize(Friend friend) {
        if (friend != null && friend.getCreatedTime() != -1){
            beginEvent(friend.getCreatedTime(), UpdateEvent.UpdateEventType.ADD_FRIENDSHIP);
            data.add(friend.getUserAcc());
            data.add(friend.getFriendAcc());
            date.setTimeInMillis(friend.getCreatedTime());
            data.add(DateGenerator.formatDateDetail(date));
            endEvent();
        }
    }

    @Override
    public void serialize(Post post) {
        beginEvent(post.getCreationDate(), UpdateEvent.UpdateEventType.ADD_POST);
        String empty = "";
        data.add(Long.parseLong(SN.formId(post.getMessageId())));
        data.add(empty);
        date.setTimeInMillis(post.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        data.add(dateString);
        if (post.getIpAddress() != null) {
            data.add(post.getIpAddress().toString());
        } else {
            data.add(empty);
        }
        if (post.getBrowserIdx() != -1){
            data.add(browserDic.getName(post.getBrowserIdx()));
        } else {
            data.add(empty);
        }
        if (post.getLanguage() != -1) {
            data.add(languageDic.getLanguagesName(post.getLanguage()));
        } else {
            data.add(empty);
        }
        if( exportText ) {
            data.add(post.getContent());
        } else {
            data.add(empty);
        }
        data.add(post.getTextSize());
        data.add(post.getAuthorId());
        data.add(Long.parseLong(SN.formId(post.getGroupId())));
        data.add(ipDic.getLocation(post.getIpAddress()));

        beginList();
        Iterator<Integer> it = post.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            list.add(tagId);
        }
        endList();
        endEvent();
    }

    @Override
    public void serialize(Like like) {
        if( like.type == 1) {
            beginEvent(like.date, UpdateEvent.UpdateEventType.ADD_LIKE_COMMENT);
        } else {
            beginEvent(like.date, UpdateEvent.UpdateEventType.ADD_LIKE_POST);
        }
        date.setTimeInMillis(like.date);
        String dateString = DateGenerator.formatDateDetail(date);
        data.add(like.user);
        data.add(Long.parseLong(SN.formId(like.messageId)));
        data.add(dateString);
        endEvent();
    }

    @Override
    public void serialize(Photo photo) {

        beginEvent(photo.getCreationDate(), UpdateEvent.UpdateEventType.ADD_POST);
        String empty = "";
        data.add(Long.parseLong(SN.formId(photo.getMessageId())));
        data.add(photo.getContent());
        date.setTimeInMillis(photo.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        data.add(dateString);
        if (photo.getIpAddress() != null) {
            data.add(photo.getIpAddress().toString());
        } else {
            data.add(empty);
        }
        if (photo.getBrowserIdx() != -1){
            data.add(browserDic.getName(photo.getBrowserIdx()));
        } else {
            data.add(empty);
        }
        data.add(empty);
        data.add(empty);
        data.add(0);
        data.add(photo.getAuthorId());
        data.add(Long.parseLong(SN.formId(photo.getGroupId())));
        data.add(ipDic.getLocation(photo.getIpAddress()));

        beginList();
        Iterator<Integer> it = photo.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            list.add(tagId);
        }
        endList();
        endEvent();
    }

    @Override
    public void serialize(Comment comment) {

        beginEvent(comment.getCreationDate(), UpdateEvent.UpdateEventType.ADD_COMMENT);
        date.setTimeInMillis(comment.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        data.add(Long.parseLong(SN.formId(comment.getMessageId())));
        data.add(dateString);
        if (comment.getIpAddress() != null) {
            data.add(comment.getIpAddress().toString());
        } else {
            String empty = "";
            data.add(empty);
        }
        if (comment.getBrowserIdx() != -1){
            data.add(browserDic.getName(comment.getBrowserIdx()));
        } else {
            String empty = "";
            data.add(empty);
        }
        if( exportText ) {
            data.add(comment.getContent());
        }
        else {
            data.add("");
        }
        data.add(comment.getTextSize());
        data.add(comment.getAuthorId());
        data.add(ipDic.getLocation(comment.getIpAddress()));
        if (comment.getReplyOf() == comment.getPostId()) {
            data.add(Long.parseLong(SN.formId(comment.getPostId())));
            data.add(new Long(-1));
        } else {
            data.add(new Long(-1));
            data.add(Long.parseLong(SN.formId(comment.getReplyOf())));
        }
        beginList();
        Iterator<Integer> it = comment.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            list.add(tagId);
        }
        endList();
        endEvent();
    }

    @Override
    public void serialize(Group group) {
        beginEvent(group.getCreatedDate(), UpdateEvent.UpdateEventType.ADD_FORUM);
        date.setTimeInMillis(group.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);

        data.add(Long.parseLong(SN.formId(group.getGroupId())));
        data.add(group.getGroupName());
        data.add(dateString);
        data.add(group.getModeratorId());

        beginList();
        Integer groupTags[] = group.getTags();
        for (int i = 0; i < groupTags.length; i ++) {
            list.add(groupTags[i]);
        }
        endList();
        endEvent();
    }

    @Override
    public void serialize(GroupMemberShip membership) {
        beginEvent(membership.getJoinDate(), UpdateEvent.UpdateEventType.ADD_FORUM_MEMBERSHIP);
        date.setTimeInMillis(membership.getJoinDate());
        String dateString = DateGenerator.formatDateDetail(date);
        data.add(Long.parseLong(SN.formId(membership.getGroupId())));
        data.add(membership.getUserId());
        data.add(dateString);
        endEvent();
    }

    @Override
    public void serialize(WorkAt workAt) {
    }

    @Override
    public void serialize(StudyAt studyAt) {
    }

    @Override
    public void serialize(Organization organization) {

    }

    @Override
    public void serialize(Tag tag) {

    }

    @Override
    public void serialize(Location location) {

    }

    @Override
    public void serialize(TagClass tagClass) {

    }
}
