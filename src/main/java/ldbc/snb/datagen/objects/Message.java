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
package ldbc.snb.datagen.objects;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.Person.PersonSummary;

import java.util.TreeSet;

abstract public class Message {

    private long messageId_;
    private long creationDate_;
    private PersonSummary author_;
    private long forumId_;
    private String content_;
    private TreeSet<Integer> tags_;
    private IP ipAddress_;
    private int browserId_;
    private int countryId_;

    public Message() {
        tags_ = new TreeSet<Integer>();
        ipAddress_ = new IP();
    }

    public Message(long messageId,
                   long creationDate,
                   PersonSummary author,
                   long forumId,
                   String content,
                   TreeSet<Integer> tags,
                   int countryId,
                   IP ipAddress,
                   int browserId
    ) {

        assert ((author.creationDate() + DatagenParams.deltaTime) <= creationDate);
        messageId_ = messageId;
        creationDate_ = creationDate;
        author_ = new PersonSummary(author);
        forumId_ = forumId;
        content_ = content;
        tags_ = new TreeSet<Integer>(tags);
        countryId_ = countryId;
        ipAddress_ = new IP(ipAddress);
        browserId_ = browserId;
    }

    public void initialize(long messageId,
                           long creationDate,
                           PersonSummary author,
                           long forumId,
                           String content,
                           TreeSet<Integer> tags,
                           int countryId,
                           IP ipAddress,
                           int browserId
    ) {
        messageId_ = messageId;
        creationDate_ = creationDate;
        author_ = new PersonSummary(author);
        forumId_ = forumId;
        content_ = content;
        tags_.clear();
        tags_.addAll(tags);
        countryId_ = countryId;
        ipAddress_.copy(ipAddress);
        browserId_ = browserId;
    }

    public long messageId() {
        return messageId_;
    }

    public void messageId(long id) {
        messageId_ = id;
    }

    public long creationDate() {
        return creationDate_;
    }

    public void creationDate(long date) {
        creationDate_ = date;
    }

    public PersonSummary author() {
        return author_;
    }

    public void authorId(PersonSummary person) {
        author_.copy(person);
    }

    public long forumId() {
        return forumId_;
    }

    public void forumId(long id) {
        forumId_ = id;
    }

    public String content() {
        return content_;
    }

    public void content(String s) {
        content_ = s;
    }

    public TreeSet<Integer> tags() {
        return tags_;
    }

    public void tags(TreeSet<Integer> tags) {
        tags_.clear();
        tags_.addAll(tags);
    }

    public IP ipAddress() {
        return ipAddress_;
    }

    public void ipAddress(IP ip) {
        ipAddress_.copy(ip);
    }

    public int browserId() {
        return browserId_;
    }

    public void browserId(int browser) {
        browserId_ = browser;
    }

    public int countryId() {
        return countryId_;
    }

    public void countryId(int l) {
        countryId_ = l;
    }
}
