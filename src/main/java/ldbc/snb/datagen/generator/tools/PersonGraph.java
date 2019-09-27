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
package ldbc.snb.datagen.generator.tools;

import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by aprat on 18/06/15.
 */
public class PersonGraph {
    private HashMap<Long, HashSet<Long>> adjacencies_;

    public PersonGraph(ArrayList<Person> persons) {
        adjacencies_ = new HashMap<Long, HashSet<Long>>();
        for (Person p : persons) {
            HashSet<Long> neighbors = new HashSet<Long>();
            for (Knows k : p.knows()) {
                neighbors.add(k.to().accountId());
            }
            adjacencies_.put(p.accountId(), neighbors);
        }
    }

    public PersonGraph(PersonGraph graph) {
        adjacencies_ = new HashMap<Long, HashSet<Long>>();
        for (Long l : graph.adjacencies_.keySet()) {
            adjacencies_.put(l, new HashSet<Long>(graph.adjacencies_.get(l)));
        }
    }

    public Set<Long> persons() {
        return adjacencies_.keySet();
    }

    public Set<Long> neighbors(Long person) {
        return adjacencies_.get(person);
    }
}
