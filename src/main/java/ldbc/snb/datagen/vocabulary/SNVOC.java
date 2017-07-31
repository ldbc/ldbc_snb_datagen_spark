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
package ldbc.snb.datagen.vocabulary;

/**
 * LDBC social network vocabulary namespace used in the serialization process.
 */
public class SNVOC {

    public static final String NAMESPACE = "http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/";
    public static final String PREFIX = "snvoc:";


    //person
    public static final String Person = PREFIX + "Person";
    public static final String creationDate = PREFIX + "creationDate";
    public static final String firstName = PREFIX + "firstName";
    public static final String lastName = PREFIX + "lastName";
    public static final String gender = PREFIX + "gender";
    public static final String birthday = PREFIX + "birthday";
    public static final String email = PREFIX + "email";
    public static final String speaks = PREFIX + "speaks";
    public static final String browser = PREFIX + "browserUsed";
    public static final String ipaddress = PREFIX + "locationIP";
    public static final String locatedIn = PREFIX + "isLocatedIn";
    public static final String studyAt = PREFIX + "studyAt";
    public static final String workAt = PREFIX + "workAt";
    public static final String hasInterest = PREFIX + "hasInterest";
    public static final String like = PREFIX + "likes";
    public static final String knows = PREFIX + "knows";
    public static final String follows = PREFIX + "follows";
    public static final String classYear = PREFIX + "classYear";
    public static final String workFrom = PREFIX + "workFrom";
    public static final String hasOrganisation = PREFIX + "hasOrganisation";
    public static final String hasPost = PREFIX + "hasPost";
    public static final String hasComment = PREFIX + "hasComment";

    //Forum
    public static final String Forum = PREFIX + "Forum";
    public static final String title = PREFIX + "title";
    public static final String hasModerator = PREFIX + "hasModerator";
    public static final String hasTag = PREFIX + "hasTag";
    public static final String hasMember = PREFIX + "hasMember";
    public static final String containerOf = PREFIX + "containerOf";
    public static final String hasPerson = PREFIX + "hasPerson";
    public static final String joinDate = PREFIX + "joinDate";

    //Post & Comment
    public static final String Post = PREFIX + "Post";
    public static final String Comment = PREFIX + "Comment";
    public static final String hasCreator = PREFIX + "hasCreator";
    public static final String content = PREFIX + "content";
    public static final String language = PREFIX + "language";
    public static final String hasImage = PREFIX + "imageFile";
    public static final String retweet = PREFIX + "retweet";
    public static final String replyOf = PREFIX + "replyOf";
    public static final String length = PREFIX + "length";

    //Others
    public static final String id = PREFIX + "id";
    public static final String Name = PREFIX + "Name";
    public static final String Organisation = PREFIX + "Organisation";
    public static final String Tag = PREFIX + "Tag";
    public static final String TagClass = PREFIX + "TagClass";
    public static final String isPartOf = PREFIX + "isPartOf";
    public static final String url = PREFIX + "url";

    /**
     * Gets the LDBC social network vocabulary prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }

    /**
     * Gets the LDBC social network vocabulary URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the LDBC social network vocabulary RDF-URL version of the input.
     */
    public static String fullprefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
}
