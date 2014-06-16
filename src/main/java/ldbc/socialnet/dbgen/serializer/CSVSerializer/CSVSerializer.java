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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.io.OutputStream;

import ldbc.socialnet.dbgen.dictionary.*;
import ldbc.socialnet.dbgen.objects.*;
import ldbc.socialnet.dbgen.serializer.Serializer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

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
        WORKAT,
        STUDYAT,
        NUMENTITIES
    }

    private EntityFieldResolver [] resolvers;
    private TreeMap<Entities,ArrayList<CSVFileDescriptor>>  files;
    private final String SEPARATOR = "|";
    private final String NEWLINE = "\n";
    private boolean compressed = false;
    private String prefix = "";
    private int reducerId = 0;


    public CSVSerializer( String prefix, int reducerId, boolean compressed, BrowserDictionary browserDictionary, LanguageDictionary languageDictionary, TagDictionary tagDictionary, IPAddressDictionary ipAddressDictionary, LocationDictionary locationDictionary, String schemaFile) {
        resolvers = new EntityFieldResolver[Entities.NUMENTITIES.ordinal()];
        resolvers[Entities.PERSON.ordinal()] = new PersonResolver(browserDictionary, languageDictionary);
        resolvers[Entities.FRIENDSHIP.ordinal()] = new FriendshipResolver();
        resolvers[Entities.LIKE.ordinal()] = new LikeResolver();
        resolvers[Entities.FORUM.ordinal()] = new ForumResolver(tagDictionary);
        resolvers[Entities.COMMENT.ordinal()] = new CommentResolver(browserDictionary,languageDictionary,ipAddressDictionary);
        resolvers[Entities.POST.ordinal()] = new PostResolver(browserDictionary,languageDictionary,ipAddressDictionary);
        resolvers[Entities.PHOTO.ordinal()] = new PhotoResolver(browserDictionary,languageDictionary,ipAddressDictionary);
        resolvers[Entities.MEMBERSHIP.ordinal()] = new MembershipResolver();
        resolvers[Entities.TAG.ordinal()] = new TagResolver();
        resolvers[Entities.TAGCLASS.ordinal()] = new TagClassResolver();
        resolvers[Entities.PLACE.ordinal()] = new PlaceResolver(locationDictionary);
        resolvers[Entities.ORGANIZATION.ordinal()] = new OrganizationResolver();
        resolvers[Entities.WORKAT.ordinal()] = new WorkAtResolver();
        resolvers[Entities.STUDYAT.ordinal()] = new StudyAtResolver();
        files = new TreeMap<Entities,ArrayList<CSVFileDescriptor>>();
        this.compressed = compressed;
        this.prefix = prefix;
        this.reducerId = reducerId;
        loadSchema(schemaFile);
    }


    public void loadSchema(String file ) {


        try {
            File schema = new File(file);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(schema);
            doc.getDocumentElement().normalize();

            // Reading XML
            NodeList nodes = doc.getElementsByTagName("entity");
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    String entityName = element.getAttribute("name");
                    ArrayList<CSVFileDescriptor> fileDescriptors = new ArrayList<CSVFileDescriptor>();
                    NodeList files = element.getElementsByTagName("file");
                    for( int j = 0; j < files.getLength(); ++j ) {
                        Node csvfile = files.item(j);
                        if( csvfile.getNodeType() == Node.ELEMENT_NODE ) {
                            Element csvFileElement= (Element) csvfile;
                            CSVFileDescriptor fileDescriptor = new CSVFileDescriptor();
                            fileDescriptor.fileName = csvFileElement.getAttribute("name");
                            fileDescriptor.fields = new ArrayList<String>();
                            NodeList fields = element.getElementsByTagName("field");
                            for( int k = 0; k < fields.getLength(); ++k ) {
                               Node field = fields.item(k);
                                if( field.getNodeType() == Node.ELEMENT_NODE ) {
                                    Element fieldElement = (Element)field;
                                    fileDescriptor.fields.add(fieldElement.getTextContent());
                                }
                            }
                         fileDescriptors.add(fileDescriptor);
                        }
                    }
                    this.files.put(Entities.valueOf(entityName),fileDescriptors);
                }
            }

            // Create files and fields
            Set<Entities> keySet = this.files.keySet();
            Iterator<Entities> it = keySet.iterator();
            while(it.hasNext()) {
                Entities entity = it.next();
                ArrayList<CSVFileDescriptor> csvFiles = this.files.get(entity);
                for( int i = 0; i < csvFiles.size(); ++i) {
                    CSVFileDescriptor csvFile = csvFiles.get(i);
                    if( compressed ) {
                        csvFile.file = new GZIPOutputStream(new FileOutputStream(prefix +"/"+csvFile.fileName +"_"+reducerId+".csv.gz"));
                    } else {
                        csvFile.file = new FileOutputStream(prefix +"/"+csvFile.fileName +"_"+reducerId+".csv");
                    }
                    writeLine(csvFile.file,csvFile.fields);
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
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
        try {
            Set<Entities> keySet = this.files.keySet();
            Iterator<Entities> it = keySet.iterator();
            while(it.hasNext()) {
                Entities entity = it.next();
                ArrayList<CSVFileDescriptor> csvFiles = this.files.get(entity);
                for( int i = 0; i < csvFiles.size(); ++i) {
                    CSVFileDescriptor csvFile = csvFiles.get(i);
                    csvFile.file.close();
                }
            }
        } catch (Exception ex) {

        }
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
    public void serialize(WorkAt workAt) {
        this.<WorkAt>serializeFile(resolvers[Entities.WORKAT.ordinal()],workAt,files.get(Entities.WORKAT));
    }

    @Override
    public void serialize(StudyAt studyAt) {
        this.<StudyAt>serializeFile(resolvers[Entities.STUDYAT.ordinal()],studyAt,files.get(Entities.STUDYAT));
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
