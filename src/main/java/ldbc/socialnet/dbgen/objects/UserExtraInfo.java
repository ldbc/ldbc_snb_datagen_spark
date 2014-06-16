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
package ldbc.socialnet.dbgen.objects;

import java.util.HashMap;
import java.util.TreeSet;
import java.util.Set;
import java.util.Vector;

public class UserExtraInfo {
	String 				gender;
	TreeSet<String>     email;
	Vector<Integer>    languages; 
	String 				firstName;
	String 				lastName; 
	int                 locationId;
	String 				location;
	double				latt; 
	double				longt;
	long 				university = -1;
	HashMap<Long, Long> companies;
	long               classYear; 				// When graduate from the institute
	RelationshipStatus 	status;

	public UserExtraInfo() {
	    email = new TreeSet<String>();
	    companies = new HashMap<Long, Long>();
	}
	public long getClassYear() {
		return classYear;
	}
	public void setClassYear(long classYear) {
		this.classYear = classYear;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public TreeSet<String> getEmail() {
		return email;
	}
	public void addEmail(String email) {
		this.email.add(email);
	}
	public Vector<Integer> getLanguages() {
        return languages;
    }
    public void setLanguages(Vector<Integer> languages) {
        this.languages = languages;
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
	public int getLocationId() {
        return locationId;
    }
    public void setLocationId(int locationId) {
        this.locationId = locationId;
    }
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public long getUniversity() {
		return university;
	}
	public void setUniversity( long university ) {
		this.university = university;
	}
	public Set<Long> getCompanies() {
		return companies.keySet();
	}
	public void addCompany(long company, long workFrom) {
		this.companies.put(company, workFrom);
	}
	public long getWorkFrom(long company) {
		return companies.get(company);
	}
	public double getLatt() {
		return latt;
	}
	public void setLatt(double latt) {
		this.latt = latt;
	}
	public double getLongt() {
		return longt;
	}
	public void setLongt(double longt) {
		this.longt = longt;
	}

}

