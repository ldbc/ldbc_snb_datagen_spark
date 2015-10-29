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
    private HashMap<Long,HashSet<Long>> adjacencies_;
    public PersonGraph(ArrayList<Person> persons) {
        adjacencies_ = new HashMap<Long,HashSet<Long>>();
        for( Person p : persons) {
            HashSet<Long> neighbors = new HashSet<Long>();
            for (Knows k: p.knows()) {
                neighbors.add(k.to().accountId());
            }
            adjacencies_.put(p.accountId(),neighbors);
        }
    }

    public PersonGraph(PersonGraph graph) {
        adjacencies_ = new HashMap<Long,HashSet<Long>>();
        for(Long l : graph.adjacencies_.keySet()) {
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
