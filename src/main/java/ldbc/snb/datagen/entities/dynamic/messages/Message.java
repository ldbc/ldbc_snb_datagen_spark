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
package ldbc.snb.datagen.entities.dynamic.messages;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.DynamicActivity;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;

import java.util.ArrayList;
import java.util.List;

abstract public class Message implements DynamicActivity {

    private boolean isExplicitlyDeleted;
    private long messageId;
    private long creationDate;
    private long deletionDate;
    private PersonSummary author;
    private long forumId;
    private String content;
    private List<Integer> tags;
    private IP ipAddress;
    private int browserId;
    private int countryId;

    public Message() {
        tags = new ArrayList<>();
        ipAddress = new IP();
    }

    public Message(long messageId, long creationDate, long deletionDate, PersonSummary author, long forumId,
                   String content, List<Integer> tags, int countryId, IP ipAddress, int browserId,
                   boolean isExplicitlyDeleted
    ) {
        assert ((author.getCreationDate() + DatagenParams.delta) <= creationDate);
        this.messageId = messageId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.author = new PersonSummary(author);
        this.forumId = forumId;
        this.content = content;
        this.tags = new ArrayList<>(tags);
        this.countryId = countryId;
        this.ipAddress = new IP(ipAddress);
        this.browserId = browserId;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public void initialize(long messageId, long creationDate, long deletionDate, PersonSummary author, long forumId,
                           String content, List<Integer> tags, int countryId, IP ipAddress, int browserId,
                           boolean isExplicitlyDeleted
    ) {
        this.messageId = messageId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.author = new PersonSummary(author);
        this.forumId = forumId;
        this.content = content;
        this.tags.clear();
        this.tags.addAll(tags);
        this.countryId = countryId;
        this.ipAddress.copy(ipAddress);
        this.browserId = browserId;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
    }

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }

    public long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    public PersonSummary getAuthor() {
        return author;
    }

    public void setAuthor(PersonSummary author) {
        this.author = author;
    }

    public long getForumId() {
        return forumId;
    }

    public void setForumId(long forumId) {
        this.forumId = forumId;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public List<Integer> getTags() {
        return tags;
    }

    public void setTags(List<Integer> tags) {
        this.tags = tags;
    }

    public IP getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(IP ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getBrowserId() {
        return browserId;
    }

    public void setBrowserId(int browserId) {
        this.browserId = browserId;
    }

    public int getCountryId() {
        return countryId;
    }

    public void setCountryId(int countryId) {
        this.countryId = countryId;
    }
}
