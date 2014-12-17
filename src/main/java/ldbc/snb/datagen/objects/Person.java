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
    public short maxNumKnows;
    public TreeSet<Knows> knows;
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
    public long randomId;

    public TreeSet<String> emails;
    public ArrayList<Integer> languages;
    public String firstName;
    public String lastName;
    public HashMap<Long, Long> companies;
    public long classYear;

    public Person(){
        this.knows = new TreeSet<Knows>();
        this.emails = new TreeSet<String>();
        this.interests = new TreeSet<Integer>();
        this.languages = new ArrayList<Integer>();
        this.companies = new HashMap<Long, Long>();
    }

    public Person( Person p ) {
        this.knows = new TreeSet<Knows>();
        this.emails = new TreeSet<String>();
        this.interests = new TreeSet<Integer>();
        this.languages = new ArrayList<Integer>();
        this.companies = new HashMap<Long, Long>();

        this.accountId = p.accountId;
        this.creationDate = p.creationDate;
        this.maxNumKnows = p.maxNumKnows;
        for( Knows k : p.knows ) {
            this.knows.add(new Knows(k));
        }

        this.agentId = p.agentId;
        this.browserId = p.browserId;
        this.ipAddress = new IP(p.ipAddress);

        this.countryId = p.countryId;
        this.cityId = p.cityId;
        this.wallId = p.wallId;
        for( Integer t : p.interests.descendingSet()) {
            this.interests.add(t);
        }
        this.mainInterest = p.mainInterest;

        this.universityLocationId = p.universityLocationId;
        this.gender = p.gender;
        this.birthDay = p.birthDay;
        this.isLargePoster = p.isLargePoster;
        this.randomId = p.randomId;

        for( String s : p.emails.descendingSet()) {
            this.emails.add(new String(s));
        }

        for( Integer i : p.languages) {
            this.languages.add(i);
        }

        this.firstName = new String(p.firstName);
        this.lastName = new String(p.lastName);
        for( Map.Entry<Long,Long> c : companies.entrySet()) {
            this.companies.put(c.getKey(),c.getValue());
        }
        this.classYear = p.classYear;

    }


    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {

        accountId = stream.readLong();
        creationDate = stream.readLong();
        maxNumKnows = stream.readShort();
        int numFriends = stream.readShort();
        for (int i = 0; i < numFriends; i++) {
            Knows fr = new Knows();
            fr.readFields(stream);
            knows.add(fr);
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
        randomId = stream.readLong();

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
        stream.writeShort(maxNumKnows);
        stream.writeShort(knows.size());

        for( Knows f : knows ){
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
        stream.writeLong(randomId);

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
            stream.writeLong(e.getValue());
        }
        stream.writeLong(classYear);
    }

    public void readFields(DataInput arg0) throws IOException {
        accountId = arg0.readLong();
        creationDate = arg0.readLong();
        maxNumKnows = arg0.readShort();
        int numFriends = arg0.readShort();
        knows = new TreeSet<Knows>();
        for (int i = 0; i < numFriends; i++) {
            Knows fr = new Knows();
            fr.readFields(arg0);
            knows.add(fr);
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
        randomId = arg0.readLong();

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

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(accountId);
        arg0.writeLong(creationDate);
        arg0.writeShort(maxNumKnows);
        arg0.writeShort(knows.size());

        for( Knows f : knows ) {
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
        arg0.writeLong(randomId);

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
            arg0.writeLong(e.getValue());
        }
        arg0.writeLong(classYear);
    }
}
