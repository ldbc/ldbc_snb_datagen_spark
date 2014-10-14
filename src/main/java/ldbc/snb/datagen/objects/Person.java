package ldbc.snb.datagen.objects;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by aprat on 10/8/14.
 */
public class Person implements Serializable, Writable {

    public long accountId;
    public long creationDate;
    public short maxNumFriends;
    public TreeSet<Friend> friends;
    public int agentId;
    public int browserId;
    public IP ipAddress;
    public int countryId;
    public int cityId;
    public long wallId;
    public TreeSet<Integer> interests;
    public int mainInterest;
    public int universityLocationId;
    public byte gender;
    public long birthDay;
    public boolean isLargePoster;

    public TreeSet<String> emails;
    public ArrayList<Integer> languages;
    public String firstName;
    public String lastName;
    public HashMap<Long, Long> companies;
    public long classYear;

    public Person(){
        friends = new TreeSet<Friend>();
        emails = new TreeSet<String>();
        interests = new TreeSet<Integer>();
        languages = new ArrayList<Integer>();
        companies = new HashMap<Long, Long>();
    }
    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {

        accountId = stream.readLong();
        creationDate = stream.readLong();
        maxNumFriends = stream.readShort();
        int numFriends = stream.readShort();
        for (int i = 0; i < numFriends; i++) {
            Friend fr = new Friend();
            fr.readFields(stream);
            friends.add(fr);
        }

        agentId = stream.readInt();
        browserId = stream.readInt();

        int ip = stream.readInt();
        int mask = stream.readInt();
        ipAddress = new IP(ip, mask);

        countryId = stream.readInt();
        cityId = stream.readInt();
        wallId = stream.readLong();

        byte numOfTags = stream.readByte();
        interests = new TreeSet<Integer>();
        for (byte i = 0; i < numOfTags; i++) {
            interests.add(stream.readInt());
        }
        mainInterest = stream.readInt();

        universityLocationId = stream.readInt();
        gender = stream.readByte();
        birthDay = stream.readLong();
        isLargePoster = stream.readBoolean();

        int numEmails = stream.readInt();
        emails = new TreeSet<String>();
        for( int i = 0; i < numEmails; ++i ) {
            emails.add(stream.readUTF());
        }
        int numLanguages = stream.readInt();
        languages = new ArrayList<Integer>();
        for( int i = 0; i < numLanguages; ++i ) {
            languages.add(stream.readInt());
        }
        firstName = stream.readUTF();
        lastName = stream.readUTF();
        int numCompanies = stream.readInt();
        companies = new HashMap<Long,Long>();
        for( int i = 0; i < numCompanies; ++i) {
            companies.put(stream.readLong(),stream.readLong());
        }
        classYear = stream.readLong();
    }

    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {

        stream.writeLong(accountId);
        stream.writeLong(creationDate);
        stream.writeShort(maxNumFriends);
        stream.writeShort(friends.size());

        for( Friend f : friends ){
            f.write(stream);
        }

        stream.writeInt(agentId);
        stream.writeInt(browserId);

        stream.writeInt(ipAddress.getIp());
        stream.writeInt(ipAddress.getMask());

        stream.writeInt(countryId);
        stream.writeInt(cityId);
        stream.writeLong(wallId);

        stream.writeByte((byte) interests.size());
        Iterator<Integer> iter2 = interests.iterator();
        while (iter2.hasNext()) {
            stream.writeInt(iter2.next());
        }
        stream.writeInt(mainInterest);

        stream.writeInt(universityLocationId);
        stream.writeByte(gender);
        stream.writeLong(birthDay);
        stream.writeBoolean(isLargePoster);

        stream.writeInt(emails.size());
        for( String s : emails ) {
            stream.writeUTF(s);
        }
        stream.writeInt(languages.size());
        for( Integer l : languages ) {
            stream.writeInt(l);
        }
        stream.writeUTF(firstName);
        stream.writeUTF(lastName);
        stream.writeInt(companies.size());
        for( Map.Entry<Long,Long> e : companies.entrySet()) {
            stream.writeLong(e.getKey());
            stream.writeLong(e.getKey());
        }
        stream.writeLong(classYear);
    }

    public void readFields(DataInput arg0) throws IOException {
        accountId = arg0.readLong();
        creationDate = arg0.readLong();
        maxNumFriends = arg0.readShort();
        int numFriends = arg0.readShort();
        friends = new TreeSet<Friend>();
        for (int i = 0; i < numFriends; i++) {
            Friend fr = new Friend();
            fr.readFields(arg0);
            friends.add(fr);
        }

        agentId = arg0.readInt();
        browserId = arg0.readInt();

        int ip = arg0.readInt();
        int mask = arg0.readInt();
        ipAddress = new IP(ip, mask);

        countryId = arg0.readInt();
        cityId = arg0.readInt();
        wallId = arg0.readLong();

        byte numTags = arg0.readByte();
        interests = new TreeSet<Integer>();
        for (byte i = 0; i < numTags; i++) {
            interests.add(arg0.readInt());
        }
        mainInterest = arg0.readInt();

        universityLocationId = arg0.readInt();
        gender = arg0.readByte();
        birthDay = arg0.readLong();
        isLargePoster = arg0.readBoolean();

        int numEmails = arg0.readInt();
        emails = new TreeSet<String>();
        for( int i = 0; i < numEmails; ++i ) {
            emails.add(arg0.readUTF());
        }
        int numLanguages = arg0.readInt();
        languages = new ArrayList<Integer>();
        for( int i = 0; i < numLanguages; ++i ) {
            languages.add(arg0.readInt());
        }
        firstName = arg0.readUTF();
        lastName = arg0.readUTF();
        int numCompanies = arg0.readInt();
        companies = new HashMap<Long,Long>();
        for( int i = 0; i < numCompanies; ++i) {
            companies.put(arg0.readLong(),arg0.readLong());
        }
        classYear = arg0.readLong();
    }

    public void copyFields(Person person) {
        accountId = person.accountId;
        creationDate = person.creationDate;
        maxNumFriends = person.maxNumFriends;
        friends.clear();
        friends.addAll(person.friends);
        agentId = person.agentId;
        browserId = person.browserId;
        ipAddress = person.ipAddress;
        countryId = person.countryId;
        cityId = person.cityId;
        wallId = person.wallId;
        interests = person.interests;
        mainInterest = person.mainInterest;
        universityLocationId = person.universityLocationId;
        gender = person.gender;
        birthDay = person.birthDay;
        isLargePoster = person.isLargePoster;

        emails.clear();
        emails.addAll(person.emails);
        languages.clear();
        languages.addAll(person.languages);
        firstName = person.firstName;
        lastName = person.lastName;
        companies.clear();
        companies.putAll(companies);
        classYear = person.classYear;
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(accountId);
        arg0.writeLong(creationDate);
        arg0.writeShort(maxNumFriends);
        arg0.writeShort(friends.size());

        for( Friend f : friends ) {
            f.write(arg0);
        }

        arg0.writeInt(agentId);
        arg0.writeInt(browserId);
        arg0.writeInt(ipAddress.getIp());
        arg0.writeInt(ipAddress.getMask());

        arg0.writeInt(countryId);
        arg0.writeInt(cityId);
        arg0.writeLong(wallId);

        arg0.writeByte((byte) interests.size());
        Iterator<Integer> iter2 = interests.iterator();
        while (iter2.hasNext()) {
            arg0.writeInt(iter2.next());
        }
        arg0.writeInt(mainInterest);
        arg0.writeInt(universityLocationId);
        arg0.writeByte(gender);
        arg0.writeLong(birthDay);
        arg0.writeBoolean(isLargePoster);

        arg0.writeInt(emails.size());
        for( String s : emails ) {
            arg0.writeUTF(s);
        }
        arg0.writeInt(languages.size());
        for( Integer l : languages ) {
            arg0.writeInt(l);
        }
        arg0.writeUTF(firstName);
        arg0.writeUTF(lastName);
        arg0.writeInt(companies.size());
        for( Map.Entry<Long,Long> e : companies.entrySet()) {
            arg0.writeLong(e.getKey());
            arg0.writeLong(e.getKey());
        }
        arg0.writeLong(classYear);
    }
}
