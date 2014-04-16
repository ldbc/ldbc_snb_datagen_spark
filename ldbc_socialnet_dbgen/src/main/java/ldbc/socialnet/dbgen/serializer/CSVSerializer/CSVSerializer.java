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
package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.io.OutputStream;

import ldbc.socialnet.dbgen.objects.*;
import ldbc.socialnet.dbgen.serializer.Serializer;

/**
 * CSV serializer.
 */
public class CSVSerializer implements Serializer {


    enum Entities {
        PERSON,
        FRIENDSHIP,
        LIKE,
        FORUM,
        COMMENT,
        POST,
        PHOTO,
        MEMBERSHIP,
        TAG,
        TAGCLASS,
        PLACE,
        ORGANIZATION,
        NUMENTITIES
    }

    private EntityFieldResolver [] resolvers;
    private TreeMap<Entities,ArrayList<CSVFileDescriptor>>  files;
    private final String SEPARATOR = "|";
    private final String NEWLINE = "\n";


    public CSVSerializer() {
        resolvers = new EntityFieldResolver[Entities.NUMENTITIES.ordinal()];
        resolvers[Entities.PERSON.ordinal()] = new PersonResolver();
        resolvers[Entities.FRIENDSHIP.ordinal()] = new FriendshipResolver();
        resolvers[Entities.LIKE.ordinal()] = new LikeResolver();
        resolvers[Entities.FORUM.ordinal()] = new ForumResolver();
        resolvers[Entities.COMMENT.ordinal()] = new CommentResolver();
        resolvers[Entities.POST.ordinal()] = new PostResolver();
        resolvers[Entities.PHOTO.ordinal()] = new PhotoResolver();
        resolvers[Entities.MEMBERSHIP.ordinal()] = new MembershipResolver();
        resolvers[Entities.TAG.ordinal()] = new TagResolver();
        resolvers[Entities.TAGCLASS.ordinal()] = new TagClassResolver();
        resolvers[Entities.PLACE.ordinal()] = new PlaceResolver();
        resolvers[Entities.ORGANIZATION.ordinal()] = new OrganizationResolver();
        files = new TreeMap<Entities,ArrayList<CSVFileDescriptor>>();
    }

    public void loadSchema(String file ) {

    }

    private <T> void serializeFile ( EntityFieldResolver<T> resolver, T entity, ArrayList<CSVFileDescriptor> fileDescriptors) {
        for( CSVFileDescriptor fileDescriptor : fileDescriptors ) {
            ArrayList<ArrayList<String>> data = new ArrayList<ArrayList<String>> ();
            for( String field : fileDescriptor.fields ) {
               data.add(resolver.queryField(field,entity));
            }
            int numLines = 1;
            for( ArrayList<String> column : data) {
                numLines *= column.size();
            }
            ArrayList<ArrayList<String>> lines = new ArrayList<ArrayList<String>>();
            for( int i = 0; i < numLines; ++i ) {
                lines.add(new ArrayList<String>());
            }

            for( ArrayList<String> column : data) {
                for( int i = 0; i < lines.size(); ++i ) {
                   lines.get(i).add( column.get( i % lines.size() ));
                }
            }
            for( ArrayList<String> line: lines ) {
                writeLine( fileDescriptor.file,line );
            }
        }
    }

    private void writeLine( OutputStream os, ArrayList<String> line ) {
        StringBuffer result = new StringBuffer();
        result.append(line.get(0));
        for (int i = 1; i < line.size(); i++) {
            result.append(SEPARATOR);
            result.append(line.get(i));
        }
        result.append(SEPARATOR);
        result.append(NEWLINE);

        try {
            byte [] dataArray = result.toString().getBytes("UTF8");
            os.write(dataArray);
        } catch (IOException e) {
            System.out.println("Cannot write to output file ");
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public Long unitsGenerated() {
        return null;
    }

    @Override
    public void serialize( UserInfo info ) {
        this.<UserInfo>serializeFile(resolvers[Entities.PERSON.ordinal()],info,files.get(Entities.PERSON));
    }

    @Override
    public void serialize( Friend friend ) {
        this.<Friend>serializeFile(resolvers[Entities.FRIENDSHIP.ordinal()],friend,files.get(Entities.FRIENDSHIP));
    }

    @Override
    public void serialize(Post post) {
        this.<Post>serializeFile(resolvers[Entities.POST.ordinal()],post,files.get(Entities.POST));
    }

    @Override
    public void serialize( Like like ) {
        this.<Like>serializeFile(resolvers[Entities.LIKE.ordinal()],like,files.get(Entities.LIKE));
    }

    @Override
    public void serialize(Photo photo) {
        this.<Photo>serializeFile(resolvers[Entities.PHOTO.ordinal()],photo,files.get(Entities.PHOTO));
    }

    @Override
    public void serialize(Comment comment) {
        this.<Comment>serializeFile(resolvers[Entities.COMMENT.ordinal()],comment,files.get(Entities.COMMENT));
    }

    @Override
    public void serialize(Group group) {
        this.<Group>serializeFile(resolvers[Entities.FORUM.ordinal()],group,files.get(Entities.FORUM));
    }

    @Override
    public void serialize( GroupMemberShip membership ) {
        this.<GroupMemberShip>serializeFile(resolvers[Entities.MEMBERSHIP.ordinal()],membership,files.get(Entities.MEMBERSHIP));
    }

    @Override
    public void serialize(Organization organization) {
        this.<Organization>serializeFile(resolvers[Entities.ORGANIZATION.ordinal()],organization,files.get(Entities.ORGANIZATION));
    }

    @Override
    public void serialize(Tag tag) {
        this.<Tag>serializeFile(resolvers[Entities.TAG.ordinal()],tag,files.get(Entities.TAG));
    }

    @Override
    public void serialize(Location location) {
        this.<Location>serializeFile(resolvers[Entities.PLACE.ordinal()],location,files.get(Entities.PLACE));
    }

    @Override
    public void serialize(TagClass tagClass) {
        this.<TagClass>serializeFile(resolvers[Entities.TAGCLASS.ordinal()],tagClass,files.get(Entities.TAGCLASS));
    }
}
