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
package ldbc.snb.datagen.entities.dynamic.person;

import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class Person implements Writable {

    private boolean isExplicitlyDeleted;
    private long accountId;
    private long creationDate;
    private long deletionDate;
    private long maxNumKnows;
    private TreeSet<Knows> knows;
    private int browserId;
    private IP ipAddress;
    private int countryId;
    private int cityId;
    private TreeSet<Integer> interests;
    private int mainInterest;
    private int universityLocationId;
    private byte gender;
    private long birthday;
    private boolean isLargePoster;
    private long randomId;

    private TreeSet<String> emails;
    private List<Integer> languages;
    private String firstName;
    private String lastName;
    private Map<Long, Long> companies;
    private long classYear;

    public interface PersonSimilarity {
        float similarity(Person personA, Person personB);
    }

    public Person() {
        knows = new TreeSet<>();
        emails = new TreeSet<>();
        interests = new TreeSet<>();
        languages = new ArrayList<>();
        companies = new HashMap<>();
        ipAddress = new IP();
    }

    public Person(Person p) {

        isExplicitlyDeleted = p.isExplicitlyDeleted;
        knows = new TreeSet<>();
        emails = new TreeSet<>();
        interests = new TreeSet<>();
        languages = new ArrayList<>();
        companies = new HashMap<>();

        accountId = p.getAccountId();
        creationDate = p.getCreationDate();
        deletionDate = p.getDeletionDate();
        maxNumKnows = p.getMaxNumKnows();
        for (Knows k : p.getKnows()) {
            knows.add(new Knows(k));
        }

        browserId = p.getBrowserId();
        ipAddress = new IP(p.getIpAddress());

        countryId = p.getCountryId();
        cityId = p.getCityId();
        interests.addAll(p.getInterests().descendingSet());
        mainInterest = p.getMainInterest();

        universityLocationId = p.getUniversityLocationId();
        gender = p.getGender();
        birthday = p.getBirthday();
        isLargePoster = p.getIsLargePoster();
        randomId = p.getRandomId();

        emails.addAll(p.getEmails().descendingSet());

        languages.addAll(p.getLanguages());

        firstName = p.getFirstName();
        lastName = p.getLastName();
        for (Map.Entry<Long, Long> c : p.getCompanies().entrySet()) {
            companies.put(c.getKey(), c.getValue());
        }
        classYear = p.getClassYear();

    }

    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public void setExplicitlyDeleted(boolean explicitlyDeleted) {
        isExplicitlyDeleted = explicitlyDeleted;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long id) {
        accountId = id;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public long getDeletionDate() {
        return deletionDate;
    }

    public void setCreationDate(long creationDate) {
        this.creationDate = creationDate;
    }

    public void setDeletionDate(long deletionDate) {
        this.deletionDate = deletionDate;
    }

    public long getMaxNumKnows() {
        return maxNumKnows;
    }

    public void setMaxNumKnows(long maxKnows) {
        maxNumKnows = maxKnows;
    }

    public TreeSet<Knows> getKnows() {
        return knows;
    }

    public void setKnows(TreeSet<Knows> knows) {
        this.knows.clear();
        this.knows.addAll(knows);
    }

    public int getBrowserId() {
        return browserId;
    }

    public void setBrowserId(int browserId) {
        this.browserId = browserId;
    }

    public IP getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(IP ipAddress) {
        this.ipAddress.copy(ipAddress);
    }

    public int getCountryId() {
        return countryId;
    }

    public void setCountryId(int countryId) {
        this.countryId = countryId;
    }

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    public TreeSet<Integer> getInterests() {
        return interests;
    }

    public void setInterests(TreeSet<Integer> interests) {
        this.interests.clear();
        this.interests.addAll(interests);
    }

    public int getMainInterest() {
        return mainInterest;
    }

    public void setMainInterest(int interest) {
        mainInterest = interest;
    }

    public int getUniversityLocationId() {
        return universityLocationId;
    }

    public void setUniversityLocationId(int location) {
        universityLocationId = location;
    }

    public byte getGender() {
        return gender;
    }

    public void setGender(byte gender) {
        this.gender = gender;
    }

    public long getBirthday() {
        return birthday;
    }

    public void setBirthday(long birthday) {
        this.birthday = birthday;
    }

    public boolean getIsLargePoster() {
        return isLargePoster;
    }

    public void setIsLargePoster(boolean largePoster) {
        isLargePoster = largePoster;
    }

    public long getRandomId() {
        return randomId;
    }

    public void setRandomId(long randomId) {
        this.randomId = randomId;
    }

    public TreeSet<String> getEmails() {
        return emails;
    }

    public void setEmails(TreeSet<String> emails) {
        emails.clear();
        this.emails.addAll(emails);
    }

    public List<Integer> getLanguages() {
        return languages;
    }

    public void setLanguages(List<Integer> languages) {
        this.languages.clear();
        this.languages.addAll(languages);
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Map<Long, Long> getCompanies() {
        return companies;
    }

    public long getClassYear() {
        return classYear;
    }

    public void setClassYear(long classYear) {
        this.classYear = classYear;
    }

    public void readFields(DataInput arg0) throws IOException {
        accountId = arg0.readLong();
        creationDate = arg0.readLong();
        deletionDate = arg0.readLong();
        maxNumKnows = arg0.readLong();
        int numFriends = arg0.readShort();
        knows = new TreeSet<>();
        for (int i = 0; i < numFriends; i++) {
            Knows fr = new Knows();
            fr.readFields(arg0);
            knows.add(fr);
        }

        browserId = arg0.readInt();

        ipAddress.readFields(arg0);

        countryId = arg0.readInt();
        cityId = arg0.readInt();

        byte numTags = arg0.readByte();
        interests = new TreeSet<>();
        for (byte i = 0; i < numTags; i++) {
            interests.add(arg0.readInt());
        }
        mainInterest = arg0.readInt();

        universityLocationId = arg0.readInt();
        gender = arg0.readByte();
        birthday = arg0.readLong();
        isLargePoster = arg0.readBoolean();
        randomId = arg0.readLong();

        int numEmails = arg0.readInt();
        emails = new TreeSet<>();
        for (int i = 0; i < numEmails; ++i) {
            emails.add(arg0.readUTF());
        }
        int numLanguages = arg0.readInt();
        languages = new ArrayList<>();
        for (int i = 0; i < numLanguages; ++i) {
            languages.add(arg0.readInt());
        }
        firstName = arg0.readUTF();
        lastName = arg0.readUTF();
        int numCompanies = arg0.readInt();
        companies = new HashMap<>();
        for (int i = 0; i < numCompanies; ++i) {
            companies.put(arg0.readLong(), arg0.readLong());
        }
        classYear = arg0.readLong();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(accountId);
        arg0.writeLong(creationDate);
        arg0.writeLong(deletionDate);
        arg0.writeLong(maxNumKnows);
        arg0.writeShort(knows.size());

        for (Knows f : knows) {
            f.write(arg0);
        }

        arg0.writeInt(browserId);
        ipAddress.write(arg0);

        arg0.writeInt(countryId);
        arg0.writeInt(cityId);

        arg0.writeByte((byte) interests.size());
        for (Integer interest : interests) {
            arg0.writeInt(interest);
        }
        arg0.writeInt(mainInterest);
        arg0.writeInt(universityLocationId);
        arg0.writeByte(gender);
        arg0.writeLong(birthday);
        arg0.writeBoolean(isLargePoster);
        arg0.writeLong(randomId);

        arg0.writeInt(emails.size());
        for (String s : emails) {
            arg0.writeUTF(s);
        }
        arg0.writeInt(languages.size());
        for (Integer l : languages) {
            arg0.writeInt(l);
        }
        arg0.writeUTF(firstName);
        arg0.writeUTF(lastName);
        arg0.writeInt(companies.size());
        for (Map.Entry<Long, Long> e : companies.entrySet()) {
            arg0.writeLong(e.getKey());
            arg0.writeLong(e.getValue());
        }
        arg0.writeLong(classYear);
    }

    public static PersonSimilarity personSimilarity;
}
