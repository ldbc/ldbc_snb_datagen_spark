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
    private FSDataOutputStream hdfsOutput;
    private ArrayList<String> data;
    private ArrayList<String> list;
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

    public UpdateEventSerializer( String outputDir, String outputFileName,boolean exportText, boolean compress, TagDictionary tagDic, BrowserDictionary browserDic, LanguageDictionary languageDic, IPAddressDictionary ipDic, Statistics statistics) {
        this.data = new ArrayList<String>();
        this.list = new ArrayList<String>();
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
            if( compress ) {
                this.fileOutputStream = new GZIPOutputStream(new FileOutputStream(outputDir + "/" + outputFileName +".gz"));
            } else {
                this.fileOutputStream = new FileOutputStream(outputDir + "/" + outputFileName );
            }
            hdfsOutput = new FSDataOutputStream(this.fileOutputStream, new FileSystem.Statistics(null));
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
        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_LIKE_POST.toString());
        params.add("UserId");
        params.add("PostId");
        params.add("CreationDate");
        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_LIKE_COMMENT.toString());
        params.add("UserId");
        params.add("CommentId");
        params.add("CreationDate");
        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_FORUM.toString());
        params.add("ForumId");
        params.add("ForumTitle");
        params.add("CreationDate");
        params.add("ModeratorId");
        params.add("Tags");
        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_FORUM_MEMBERSHIP.toString());
        params.add("ForumId");
        params.add("UserId");
        params.add("CreationDate");
        statistics.eventParams.add(params);

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
        statistics.eventParams.add(params);

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
        statistics.eventParams.add(params);

        params = new ArrayList<String>();
        params.add(UpdateEvent.UpdateEventType.ADD_FRIENDSHIP.toString());
        params.add("UserId");
        params.add("UserId");
        params.add("CreationDate");
        statistics.eventParams.add(params);
    }

    private void writeEvent( UpdateEvent event ) {
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
            hdfsOutput.write(string.toString().getBytes("UTF8"));
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
        currentEvent.eventData = formatData(data);
        writeEvent(currentEvent);
    }

    private void beginList() {
        list.clear();
    }

    private void endList() {
        data.add(createList(list));
    }


    public void close() {
        statistics.minUpdateStream.add(Long.toString(minDate));
        date.setTimeInMillis(minDate);
        statistics.minUpdateStream.add(DateGenerator.formatDateDetail(date));

        statistics.maxUpdateStream.add(Long.toString(maxDate));
        date.setTimeInMillis(maxDate);
        statistics.maxUpdateStream.add(DateGenerator.formatDateDetail(date));

        try {
            fileOutputStream.flush();
            hdfsOutput.flush();
            fileOutputStream.close();
            hdfsOutput.close();
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }


    private String formatData( ArrayList<String> data) {
        StringBuffer string = new StringBuffer(1024);
        string.append("{");
        for( int i = 0; i < data.size() - 1; ++i ) {
            string.append(data.get(i));
            string.append(";");
        }
        string.append(data.get(data.size()-1));
        string.append("}");
        return string.toString();
    }

    private String createList( ArrayList<String> data) {
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

    @Override
    public Long unitsGenerated() {
        return new Long(0);
    }

    @Override
    public void serialize(UserInfo info) {

        beginEvent(info.user.getCreationDate(), UpdateEvent.UpdateEventType.ADD_PERSON);
        data.add(Long.toString(info.user.getAccountId()));
        data.add(info.extraInfo.getFirstName());
        data.add(info.extraInfo.getLastName());
        data.add(info.extraInfo.getGender());
        date.setTimeInMillis(info.user.getBirthDay());
        String dateString = DateGenerator.formatDate(date);
        data.add(dateString);
        date.setTimeInMillis(info.user.getCreationDate());
        dateString = DateGenerator.formatDate(date);
        data.add(dateString);
        if (info.user.getIpAddress() != null) {
            data.add(info.user.getIpAddress().toString());
        } else {
            String empty = "";
            data.add(empty);
        }
        if (info.user.getBrowserIdx() >= 0) {
            data.add(browserDic.getName(info.user.getBrowserIdx()));
        } else {
            String empty = "";
            data.add(empty);
        }
        data.add(Integer.toString(info.extraInfo.getLocationId()));
        ArrayList<String> languages = new ArrayList<String>();
        Vector<Integer> userLang = info.extraInfo.getLanguages();
        for (int i = 0; i < languages.size(); i++) {
            languages.add(languageDic.getLanguagesName(userLang.get(i)));
        }
        data.add(createList(languages));

        beginList();
        Iterator<String> itString = info.extraInfo.getEmail().iterator();
        while (itString.hasNext()){
            list.add(itString.next());
        }
        endList();

        beginList();
        Iterator<Integer> itInteger = info.user.getSetOfTags().iterator();
        while (itInteger.hasNext()){
            Integer interestIdx = itInteger.next();
            list.add(Integer.toString(interestIdx));
        }
        endList();


        beginList();
        long universityId = info.extraInfo.getUniversity();
        if ( universityId != -1){
            if (info.extraInfo.getClassYear() != -1 ) {
                ArrayList<String> studyAtData = new ArrayList<String>();
                date.setTimeInMillis(info.extraInfo.getClassYear());
                dateString = DateGenerator.formatYear(date);
                studyAtData.add(Long.toString(universityId));
                studyAtData.add(dateString);
                list.add(formatData(studyAtData));
            }
        }
        endList();

        beginList();
        Iterator<Long> it = info.extraInfo.getCompanies().iterator();
        while (it.hasNext()) {
            long companyId = it.next();
            date.setTimeInMillis(info.extraInfo.getWorkFrom(companyId));
            ArrayList<String> workAtData = new ArrayList<String>();
            dateString = DateGenerator.formatYear(date);
            workAtData.add(Long.toString(companyId));
            workAtData.add(dateString);
            list.add(formatData(workAtData));
        }
        endList();
        endEvent();
    }

    @Override
    public void serialize(Friend friend) {
        if (friend != null && friend.getCreatedTime() != -1){
            beginEvent(friend.getCreatedTime(), UpdateEvent.UpdateEventType.ADD_FRIENDSHIP);
            data.add(Long.toString(friend.getUserAcc()));
            data.add(Long.toString(friend.getFriendAcc()));
            date.setTimeInMillis(friend.getCreatedTime());
            data.add(DateGenerator.formatDateDetail(date));
            endEvent();
        }
    }

    @Override
    public void serialize(Post post) {
        beginEvent(post.getCreationDate(), UpdateEvent.UpdateEventType.ADD_POST);
        String empty = "";
        data.add(SN.formId(post.getMessageId()));
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
        data.add(Integer.toString(post.getTextSize()));
        data.add(Long.toString(post.getAuthorId()));
        data.add(SN.formId(post.getGroupId()));
        data.add(Integer.toString(ipDic.getLocation(post.getIpAddress())));

        beginList();
        Iterator<Integer> it = post.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            list.add(Integer.toString(tagId));
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
        data.add(Long.toString(like.user));
        data.add(SN.formId(like.messageId));
        data.add(dateString);
        endEvent();
    }

    @Override
    public void serialize(Photo photo) {

        beginEvent(photo.getCreationDate(), UpdateEvent.UpdateEventType.ADD_POST);
        String empty = "";
        data.add(SN.formId(photo.getMessageId()));
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
        data.add(Integer.toString(0));
        data.add(Long.toString(photo.getAuthorId()));
        data.add(SN.formId(photo.getGroupId()));
        data.add(Integer.toString(ipDic.getLocation(photo.getIpAddress())));

        beginList();
        Iterator<Integer> it = photo.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            list.add(Integer.toString(tagId));
        }
        endList();
        endEvent();
    }

    @Override
    public void serialize(Comment comment) {

        beginEvent(comment.getCreationDate(), UpdateEvent.UpdateEventType.ADD_COMMENT);
        date.setTimeInMillis(comment.getCreationDate());
        String dateString = DateGenerator.formatDateDetail(date);
        data.add(SN.formId(comment.getMessageId()));
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
        data.add(Integer.toString(comment.getTextSize()));
        data.add(Long.toString(comment.getAuthorId()));
        data.add(Integer.toString(ipDic.getLocation(comment.getIpAddress())));
        if (comment.getReplyOf() == comment.getPostId()) {
            data.add(SN.formId(comment.getPostId()));
            String empty = "";
            data.add(empty);
        } else {
            String empty = "";
            data.add(empty);
            data.add(SN.formId(comment.getReplyOf()));
        }
        beginList();
        Iterator<Integer> it = comment.getTags().iterator();
        while (it.hasNext()) {
            Integer tagId = it.next();
            data.add(Integer.toString(tagId));
        }
        endList();
        endEvent();
    }

    @Override
    public void serialize(Group group) {
        beginEvent(group.getCreatedDate(), UpdateEvent.UpdateEventType.ADD_FORUM);
        date.setTimeInMillis(group.getCreatedDate());
        String dateString = DateGenerator.formatDateDetail(date);

        data.add(SN.formId(group.getGroupId()));
        data.add(group.getGroupName());
        data.add(dateString);
        data.add(Long.toString(group.getModeratorId()));

        beginList();
        Integer groupTags[] = group.getTags();
        for (int i = 0; i < groupTags.length; i ++) {
            list.add(Integer.toString(groupTags[i]));
        }
        endList();
        endEvent();
    }

    @Override
    public void serialize(GroupMemberShip membership) {
        beginEvent(membership.getJoinDate(), UpdateEvent.UpdateEventType.ADD_FORUM_MEMBERSHIP);
        date.setTimeInMillis(membership.getJoinDate());
        String dateString = DateGenerator.formatDateDetail(date);
        data.add(SN.formId(membership.getGroupId()));
        data.add(Long.toString(membership.getUserId()));
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
