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
package ldbc.snb.datagen.objects.similarity;

import ldbc.snb.datagen.objects.Person;

import java.util.Set;
import java.util.TreeSet;

/**
 * Created by aprat on 22/01/16.
 */
public class InterestsSimilarity implements Person.PersonSimilarity {
    public float similarity(Person personA, Person personB) {
        Set<Integer> union = new TreeSet<Integer>(personA.interests());
        union.addAll(personB.interests());
        union.add(personA.mainInterest());
        union.add(personB.mainInterest());
        Set<Integer> intersection = new TreeSet<Integer>(personA.interests());
        intersection.retainAll(personB.interests());
        if (personA.mainInterest() == personB.mainInterest()) intersection.add(personA.mainInterest());
        return union.size() > 0 ? intersection.size() / (float) union.size() : 0;
    }
}
